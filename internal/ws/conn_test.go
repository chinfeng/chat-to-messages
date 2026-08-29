package ws

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The tests below exercise the server Conn through real HTTP upgrades on
// httptest servers, with a hand-rolled masked client side (RFC 6455 §5.3).

// dialWS opens a websocket connection against baseHTTPURL (a path suffix is
// allowed) and verifies the 101 handshake including the accept key.
func dialWS(t *testing.T, baseHTTPURL string) (net.Conn, *bufio.Reader) {
	t.Helper()
	addr := strings.TrimPrefix(baseHTTPURL, "http://")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))
	fmt.Fprintf(conn, "GET /v1/responses HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: keep-alive, Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", addr, key)
	br := bufio.NewReader(conn)
	statusLine, err := br.ReadString('\n')
	if err != nil || !strings.Contains(statusLine, "101") {
		t.Fatalf("handshake status: %q (%v)", statusLine, err)
	}
	wantAccept := base64.StdEncoding.EncodeToString(sha1Sum(key + acceptGUID))
	gotAccept := false
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
		if strings.HasPrefix(strings.ToLower(line), "sec-websocket-accept:") &&
			strings.TrimSpace(strings.SplitN(line, ":", 2)[1]) == wantAccept {
			gotAccept = true
		}
	}
	if !gotAccept {
		t.Fatal("missing/invalid Sec-WebSocket-Accept")
	}
	return conn, br
}

func sha1Sum(s string) []byte {
	sum := sha1.Sum([]byte(s))
	return sum[:]
}

// clientWrite sends one client frame (always masked — the server enforces it).
func clientWrite(t *testing.T, conn net.Conn, masked, fin bool, op byte, payload []byte) {
	t.Helper()
	b0 := op
	if fin {
		b0 |= 0x80
	}
	hdr := []byte{b0}
	maskBit := byte(0)
	if masked {
		maskBit = 0x80
	}
	switch n := len(payload); {
	case n <= 125:
		hdr = append(hdr, maskBit|byte(n))
	case n <= 0xFFFF:
		hdr = append(hdr, maskBit|126, byte(n>>8), byte(n))
	default:
		hdr = append(hdr, maskBit|127)
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(n))
		hdr = append(hdr, b[:]...)
	}
	mask := [4]byte{1, 2, 3, 4}
	if masked {
		hdr = append(hdr, mask[:]...)
	}
	out := make([]byte, len(payload))
	for i := range payload {
		out[i] = payload[i]
		if masked {
			out[i] ^= mask[i&3]
		}
	}
	if _, err := conn.Write(append(hdr, out...)); err != nil {
		t.Fatal(err)
	}
}

// clientRead returns the next server frame (opcode, payload); server frames
// are never masked.
func clientRead(t *testing.T, br *bufio.Reader) (byte, []byte) {
	t.Helper()
	var hdr [2]byte
	mustReadFull(t, br, hdr[:])
	op := hdr[0] & 0x0f
	n := uint64(hdr[1] & 0x7f)
	if hdr[1]&0x80 != 0 {
		t.Fatal("server frame must not be masked")
	}
	switch n {
	case 126:
		var ext [2]byte
		mustReadFull(t, br, ext[:])
		n = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		mustReadFull(t, br, ext[:])
		n = binary.BigEndian.Uint64(ext[:])
	}
	payload := make([]byte, n)
	mustReadFull(t, br, payload)
	return op, payload
}

func mustReadFull(t *testing.T, br *bufio.Reader, buf []byte) {
	t.Helper()
	got := 0
	for got < len(buf) {
		n, err := br.Read(buf[got:])
		got += n
		if err != nil {
			t.Fatal(err)
		}
	}
}

// echoServer upgrades and echoes every text message back until close.
func echoServer(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := Upgrade(w, r)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer c.Close()
		for {
			m, err := c.Read()
			if err != nil || m.Op == OpClose {
				return
			}
			if err := c.WriteText(m.Payload); err != nil {
				return
			}
		}
	}))
}

func TestHandshakeAndEchoSizes(t *testing.T) {
	srv := echoServer(t)
	defer srv.Close()
	conn, br := dialWS(t, srv.URL)
	defer conn.Close()

	for _, size := range []int{2, 200, 70000} {
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte('a' + i%26)
		}
		clientWrite(t, conn, true, true, OpText, payload)
		op, got := clientRead(t, br)
		if op != OpText || len(got) != size {
			t.Fatalf("size %d: op=%d len=%d", size, op, len(got))
		}
		for i := range payload {
			if got[i] != payload[i] {
				t.Fatalf("size %d: payload mismatch at %d", size, i)
			}
		}
	}
}

func TestFragmentedMessage(t *testing.T) {
	srv := echoServer(t)
	defer srv.Close()
	conn, br := dialWS(t, srv.URL)
	defer conn.Close()

	clientWrite(t, conn, true, false, OpText, []byte("hello "))
	clientWrite(t, conn, true, true, opContinuation, []byte("world"))
	op, got := clientRead(t, br)
	if op != OpText || string(got) != "hello world" {
		t.Fatalf("fragmented message: op=%d %q", op, got)
	}
}

func TestPingPongAndClose(t *testing.T) {
	srv := echoServer(t)
	defer srv.Close()
	conn, br := dialWS(t, srv.URL)
	defer conn.Close()

	clientWrite(t, conn, true, true, opPing, []byte("tic"))
	op, got := clientRead(t, br)
	if op != opPong || string(got) != "tic" {
		t.Fatalf("pong: op=%d %q", op, got)
	}

	code := make([]byte, 2)
	binary.BigEndian.PutUint16(code, 1000)
	clientWrite(t, conn, true, true, OpClose, append(code, "bye"...))
	op, got = clientRead(t, br)
	if op != OpClose {
		t.Fatalf("close echo: op=%d", op)
	}
	if binary.BigEndian.Uint16(got) != 1000 {
		t.Fatalf("close code: %v", got)
	}
}

func TestUnmaskedClientFrameRejected(t *testing.T) {
	srv := echoServer(t)
	defer srv.Close()
	conn, br := dialWS(t, srv.URL)
	defer conn.Close()

	clientWrite(t, conn, false, true, OpText, []byte("naughty"))
	// The server read fails and the handler tears the connection down.
	if _, err := br.ReadByte(); err == nil {
		// Some runtimes deliver the FIN as EOF after a successful read; a second
		// read must then fail.
		if _, err := br.ReadByte(); err == nil {
			t.Fatal("connection still readable after unmasked frame")
		}
	}
}

func TestUpgradeValidation(t *testing.T) {
	saw400 := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := Upgrade(w, r); err != nil {
			saw400 = true
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	// No Sec-WebSocket-Key.
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest || !saw400 {
		t.Fatalf("missing key must fail the upgrade: %d", resp.StatusCode)
	}
	// Wrong version.
	req, _ := http.NewRequest("GET", srv.URL, nil)
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Sec-WebSocket-Key", base64.StdEncoding.EncodeToString([]byte("0123456789abcdef")))
	req.Header.Set("Sec-WebSocket-Version", "12")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("version 12 must fail: %d", resp.StatusCode)
	}
}
