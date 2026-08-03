package openai

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestIterSSEChunksBasic(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\" there\"}}]}\n\ndata: [DONE]\n\n"
	var chunks []Chunk
	for c := range IterSSEChunks(t.Context(), strings.NewReader(body), nil) {
		chunks = append(chunks, c)
	}
	if len(chunks) != 3 {
		t.Fatalf("chunks = %d", len(chunks))
	}
	if *chunks[0].Choices[0].Delta.Content != "hi" {
		t.Errorf("c0 = %+v", chunks[0])
	}
	if !chunks[2].Done {
		t.Error("last should be [DONE] sentinel")
	}
}

func TestIterSSEChunksCRLFAndIgnoreNonData(t *testing.T) {
	body := ": keepalive comment\r\nevent: ping\r\ndata: {\"usage\":{\"prompt_tokens\":5}}\r\n\r\ndata: not-json\r\n\r\ndata: [DONE]\r\n\r\n"
	var chunks []Chunk
	for c := range IterSSEChunks(t.Context(), strings.NewReader(body), nil) {
		chunks = append(chunks, c)
	}
	if len(chunks) != 2 {
		t.Fatalf("chunks = %d: %+v", len(chunks), chunks)
	}
	if chunks[0].Usage == nil || chunks[0].Usage.PromptTokens != 5 {
		t.Errorf("usage = %+v", chunks[0].Usage)
	}
}

func TestIterSSEChunksSplitAcrossReads(t *testing.T) {
	// 分片读：data 行跨多个 Read
	reader := io.MultiReader(
		strings.NewReader("data: {\"choice"),
		strings.NewReader("s\":[{\"delta\":{\"content\":\"x\"}}]}\n\n"),
		strings.NewReader("data: [DONE]\n\n"),
	)
	var chunks []Chunk
	for c := range IterSSEChunks(t.Context(), reader, nil) {
		chunks = append(chunks, c)
	}
	if len(chunks) != 2 || chunks[0].Choices == nil {
		t.Fatalf("chunks = %d: %+v", len(chunks), chunks)
	}
}

func TestIterSSEChunksRawCapture(t *testing.T) {
	var raw strings.Builder
	body := "data: {\"choices\":[]}\n\ndata: [DONE]\n\n"
	for range IterSSEChunks(t.Context(), strings.NewReader(body), &raw) {
	}
	if raw.String() != body {
		t.Errorf("raw = %q", raw.String())
	}
}

type errReader struct{ n int }

func (r *errReader) Read(p []byte) (int, error) {
	if r.n > 0 {
		r.n--
		return copy(p, "data: {\"choices\":[]}\n\n"), nil
	}
	return 0, errors.New("connection reset")
}

func TestIterSSEChunksReadError(t *testing.T) {
	var chunks []Chunk
	for c := range IterSSEChunks(t.Context(), &errReader{n: 1}, nil) {
		chunks = append(chunks, c)
	}
	if len(chunks) != 2 {
		t.Fatalf("chunks = %d", len(chunks))
	}
	if chunks[1].Err == nil {
		t.Error("expected Err on last chunk")
	}
	if !strings.Contains(chunks[1].Err.Error(), "connection reset") {
		t.Errorf("err = %v", chunks[1].Err)
	}
}

// doneThenErrReader 先吐出 [DONE] 完成标记，下一次 Read 返回连接错误：
// 用于验证 [DONE] 之后不得再产出 Err chunk。
type doneThenErrReader struct{ n int }

func (r *doneThenErrReader) Read(p []byte) (int, error) {
	if r.n > 0 {
		r.n--
		return copy(p, "data: [DONE]\n\n"), nil
	}
	return 0, errors.New("connection reset")
}

func TestIterSSEChunksNoErrAfterDone(t *testing.T) {
	var chunks []Chunk
	for c := range IterSSEChunks(t.Context(), &doneThenErrReader{n: 1}, nil) {
		chunks = append(chunks, c)
	}
	if len(chunks) != 1 || !chunks[0].Done {
		t.Fatalf("chunks = %+v, want exactly the [DONE] sentinel", chunks)
	}
	if chunks[0].Err != nil {
		t.Errorf("no Err expected after [DONE], got %v", chunks[0].Err)
	}
}

func TestIterSSEChunksNoErrAfterDoneWithRaw(t *testing.T) {
	var raw strings.Builder
	var chunks []Chunk
	for c := range IterSSEChunks(t.Context(), &doneThenErrReader{n: 1}, &raw) {
		chunks = append(chunks, c)
	}
	if len(chunks) != 1 || !chunks[0].Done {
		t.Fatalf("chunks = %+v, want exactly the [DONE] sentinel", chunks)
	}
	if chunks[0].Err != nil {
		t.Errorf("no Err expected after [DONE], got %v", chunks[0].Err)
	}
	// dump 路径：raw 仍须完整累积原始文本
	if raw.String() != "data: [DONE]\n\n" {
		t.Errorf("raw = %q", raw.String())
	}
}

func TestChunkErrorObject(t *testing.T) {
	var c Chunk
	if err := decodeChunk(&c, `{"error":{"message":"rate limited","code":429}}`); err != nil {
		t.Fatal(err)
	}
	if c.Error == nil || c.Error.Message != "rate limited" || *c.Error.Code != 429 {
		t.Errorf("err obj = %+v", c.Error)
	}
}

// TestChunkErrorObjectStringCode: OpenRouter / newapi / GLM-family upstreams
// send string error codes like "E429" or "429". A numeric string is converted;
// a non-numeric string yields nil (the stream layer falls back to 500).
func TestChunkErrorObjectStringCode(t *testing.T) {
	var c Chunk
	if err := decodeChunk(&c, `{"error":{"message":"rate limited","code":"E429"}}`); err != nil {
		t.Fatal(err)
	}
	if c.Error == nil || c.Error.Message != "rate limited" || c.Error.Code != nil {
		t.Errorf("non-numeric string code should decode to nil: %+v", c.Error)
	}

	c = Chunk{}
	if err := decodeChunk(&c, `{"error":{"message":"x","code":"429"}}`); err != nil {
		t.Fatal(err)
	}
	if c.Error == nil || c.Error.Code == nil || *c.Error.Code != 429 {
		t.Errorf("numeric string code should convert: %+v", c.Error)
	}

	c = Chunk{}
	if err := decodeChunk(&c, `{"error":{"message":"x"}}`); err != nil {
		t.Fatal(err)
	}
	if c.Error == nil || c.Error.Code != nil {
		t.Errorf("missing code should decode to nil: %+v", c.Error)
	}
}

// TestIterSSEChunksErrorChunkStringCode: a string-code error chunk must now
// parse (previously the whole chunk failed JSON decoding and was silently
// dropped, misreading the stream as a normal completion or a retryable abort).
func TestIterSSEChunksErrorChunkStringCode(t *testing.T) {
	body := "data: {\"error\":{\"message\":\"rate limited\",\"code\":\"E429\"}}\n\ndata: [DONE]\n\n"
	var chunks []Chunk
	for c := range IterSSEChunks(t.Context(), strings.NewReader(body), nil) {
		chunks = append(chunks, c)
	}
	if len(chunks) != 2 {
		t.Fatalf("chunks = %d: %+v", len(chunks), chunks)
	}
	if chunks[0].Error == nil || chunks[0].Error.Message != "rate limited" || chunks[0].Error.Code != nil {
		t.Errorf("error chunk = %+v", chunks[0].Error)
	}
	if !chunks[1].Done {
		t.Errorf("last chunk = %+v, want [DONE]", chunks[1])
	}
}

func TestChunkToolCallDelta(t *testing.T) {
	var c Chunk
	if err := decodeChunk(&c, `{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"f","arguments":"{\"a\":"}}],"finish_reason":null}}]}`); err != nil {
		t.Fatal(err)
	}
	tc := c.Choices[0].Delta.ToolCalls[0]
	if tc.Index != 0 || *tc.ID != "call_1" || *tc.Function.Name != "f" || *tc.Function.Arguments != `{"a":` {
		t.Errorf("tc = %+v", tc)
	}
}

func decodeChunk(c *Chunk, data string) error {
	dec := json.NewDecoder(strings.NewReader(data))
	return dec.Decode(c)
}
