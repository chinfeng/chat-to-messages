// Package ws implements the server side of RFC 6455 (WebSocket) with the
// standard library only: the HTTP Upgrade handshake (http.Hijacker), masked
// client-frame parsing with continuation aggregation, unmasked server frames,
// and ping/pong/close. Extensions (permessage-deflate) and subprotocols are
// declined by omission — clients MUST fall back per RFC 6455 §9.1.
package ws

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
)

// Opcodes (RFC 6455 §5.2).
const (
	opContinuation = 0x0
	OpText         = 0x1
	OpBinary       = 0x2
	OpClose        = 0x8
	opPing         = 0x9
	opPong         = 0xA
)

// maxMessageBytes caps one assembled client message. A responses.create
// payload can carry a full conversation context, so this is generous.
const maxMessageBytes = 64 << 20

// acceptGUID is the magic string from RFC 6455 §1.3.
const acceptGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// Conn is one established websocket connection. Reads are single-goroutine
// (Read returns whole messages, reassembling continuations and answering
// pings inline); writes are mutex-serialized so a response job and the read
// loop can both write.
type Conn struct {
	raw net.Conn
	br  *bufio.Reader
	wmu sync.Mutex

	closeSent bool
}

// IsUpgrade reports whether r is a websocket upgrade request.
func IsUpgrade(r *http.Request) bool {
	return headerContains(r.Header, "connection", "upgrade") &&
		headerContains(r.Header, "upgrade", "websocket")
}

// Upgrade validates the handshake, hijacks the connection and answers 101.
// On error nothing is written — the caller answers plain HTTP instead.
func Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	if r.Method != http.MethodGet {
		return nil, errors.New("websocket upgrade requires GET")
	}
	if !IsUpgrade(r) {
		return nil, errors.New("missing/invalid Upgrade or Connection header")
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		return nil, errors.New("missing Sec-WebSocket-Key")
	}
	if v := r.Header.Get("Sec-WebSocket-Version"); v != "13" {
		return nil, errors.New("unsupported Sec-WebSocket-Version (want 13)")
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("response writer cannot hijack")
	}
	nc, rw, err := hj.Hijack()
	if err != nil {
		return nil, fmt.Errorf("hijack: %w", err)
	}
	sum := sha1.Sum([]byte(key + acceptGUID)) //nolint:gosec // SHA-1 is the protocol, not a MAC
	accept := base64.StdEncoding.EncodeToString(sum[:])
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err := rw.WriteString(resp); err != nil {
		nc.Close()
		return nil, err
	}
	if err := rw.Flush(); err != nil {
		nc.Close()
		return nil, err
	}
	return &Conn{raw: nc, br: rw.Reader}, nil
}

// Message is one complete websocket message from the client. Close carries
// the peer's close frame (payload holds the raw code+reason bytes).
type Message struct {
	Op      byte
	Payload []byte
}

// ErrClosed reports reads after a close handshake completed.
var ErrClosed = errors.New("websocket: connection closed")

// Read returns the next client message, reassembling fragmented messages and
// answering pings with pongs inline. io errors (and protocol violations)
// terminate the connection — the caller treats any error as terminal.
func (c *Conn) Read() (Message, error) {
	var data []byte
	dataOp := byte(0)
	fragmented := false
	for {
		fin, op, payload, err := c.readFrame()
		if err != nil {
			return Message{}, err
		}
		switch op {
		case opPing:
			if err := c.writeFrame(opPong, payload); err != nil {
				return Message{}, err
			}
			continue
		case opPong:
			continue
		case OpClose:
			// Echo close, then stop. Receiving a close completes the
			// handshake; further reads get ErrClosed.
			if !c.closeSent {
				resp := payload
				if len(resp) > 125 {
					resp = resp[:125]
				}
				_ = c.writeFrame(OpClose, resp)
				c.closeSent = true
			}
			c.raw.Close()
			return Message{Op: OpClose, Payload: payload}, nil
		case OpText, OpBinary:
			if fragmented {
				return Message{}, errors.New("websocket: data frame while a message is fragmented")
			}
			if fin {
				return Message{Op: op, Payload: payload}, nil
			}
			fragmented = true
			dataOp = op
			data = append(data, payload...)
		case opContinuation:
			if !fragmented {
				return Message{}, errors.New("websocket: continuation without a start frame")
			}
			data = append(data, payload...)
			if len(data) > maxMessageBytes {
				return Message{}, errors.New("websocket: message exceeds max size")
			}
			if fin {
				return Message{Op: dataOp, Payload: data}, nil
			}
		default:
			return Message{}, fmt.Errorf("websocket: unknown opcode %d", op)
		}
		if len(data) > maxMessageBytes {
			return Message{}, errors.New("websocket: message exceeds max size")
		}
	}
}

// WriteText sends one unfragmented text message.
func (c *Conn) WriteText(payload []byte) error {
	return c.writeFrame(OpText, payload)
}

// WriteClose sends a close frame (idempotent) and closes the socket.
func (c *Conn) WriteClose(code uint16, reason string) error {
	if c.closeSent {
		return nil
	}
	c.closeSent = true
	payload := make([]byte, 2+len(reason))
	binary.BigEndian.PutUint16(payload, code)
	copy(payload[2:], reason)
	err := c.writeFrame(OpClose, payload)
	_ = c.raw.Close()
	return err
}

// Close closes the underlying socket without a close frame (errors, aborts).
func (c *Conn) Close() error { return c.raw.Close() }

// readFrame parses one wire frame. Client frames must be masked (RFC 6455
// §5.3, enforced here); control-frame rules (≤125 bytes, never fragmented)
// are enforced.
func (c *Conn) readFrame() (fin bool, op byte, payload []byte, err error) {
	var hdr [2]byte
	if _, err = io.ReadFull(c.br, hdr[:]); err != nil {
		return false, 0, nil, err
	}
	fin = hdr[0]&0x80 != 0
	if hdr[0]&0x70 != 0 {
		return false, 0, nil, errors.New("websocket: RSV bits set (no extensions negotiated)")
	}
	op = hdr[0] & 0x0f
	masked := hdr[1]&0x80 != 0
	length := uint64(hdr[1] & 0x7f)
	if op >= 0x8 && !fin {
		return false, 0, nil, errors.New("websocket: fragmented control frame")
	}
	switch length {
	case 126:
		var ext [2]byte
		if _, err = io.ReadFull(c.br, ext[:]); err != nil {
			return false, 0, nil, err
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err = io.ReadFull(c.br, ext[:]); err != nil {
			return false, 0, nil, err
		}
		length = binary.BigEndian.Uint64(ext[:])
	}
	if op >= 0x8 && length > 125 {
		return false, 0, nil, errors.New("websocket: oversized control frame")
	}
	if length > maxMessageBytes {
		return false, 0, nil, errors.New("websocket: frame exceeds max size")
	}
	var mask [4]byte
	if masked {
		if _, err = io.ReadFull(c.br, mask[:]); err != nil {
			return false, 0, nil, err
		}
	} else {
		return false, 0, nil, errors.New("websocket: unmasked client frame (RFC 6455 §5.3)")
	}
	payload = make([]byte, length)
	if _, err = io.ReadFull(c.br, payload); err != nil {
		return false, 0, nil, err
	}
	for i := range payload {
		payload[i] ^= mask[i&3]
	}
	return fin, op, payload, nil
}

// writeFrame serializes one server frame (never masked, RFC 6455 §5.1)
// holding the write mutex so the read loop (pongs) and response jobs (event
// frames) can both write.
func (c *Conn) writeFrame(op byte, payload []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	var hdr []byte
	switch n := len(payload); {
	case n <= 125:
		hdr = []byte{0x80 | op, byte(n)}
	case n <= 0xFFFF:
		hdr = make([]byte, 4)
		hdr[0] = 0x80 | op
		hdr[1] = 126
		binary.BigEndian.PutUint16(hdr[2:], uint16(n))
	default:
		hdr = make([]byte, 10)
		hdr[0] = 0x80 | op
		hdr[1] = 127
		binary.BigEndian.PutUint64(hdr[2:], uint64(n))
	}
	if _, err := c.raw.Write(hdr); err != nil {
		return err
	}
	_, err := c.raw.Write(payload)
	return err
}

// headerContains reports whether the named header lists token
// (comma-separated, case-insensitive) — RFC 9110 §5.6.1.1 list fields.
func headerContains(h http.Header, name, token string) bool {
	for _, v := range h.Values(name) {
		for _, part := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}
