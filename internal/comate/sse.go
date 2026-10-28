package comate

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"wild-work/internal/provider"
	"wild-work/internal/upstream"
)

// ---------------------------------------------------------------------------
// peekStream：窥视首帧，识别 mock 错误帧
// ---------------------------------------------------------------------------

// peekStream 包裹上游 body：先读首段（至多 64KB）判断是否 mock 错误帧
//（id 前缀 chatcmpl-mock- 或 model=="mock"，见包注释实测事实 2），
// 命中则转成可分类的 StreamError（模型级 ErrNotFound，不罚整号）；
// 否则把已读字节与剩余流拼接后放行（外层再包 IdleReader 空闲兜底）。
//
// 为什么必须在流上做：mock 帧的 HTTP 状态是 200、响应头正常，handler 在
// 读到首帧之前就已把 200 + SSE 头写给客户端 —— 协议上无法在同一请求内
// 换号重试，唯一可行的补救是把中招模型冷却掉（与 StreamErrorClassifier
// 的设计动机一致）。
type peekStream struct {
	pre   *bytes.Reader // 已窥视的前段（含 IdleReader 包装前的原始字节）
	rest  io.Reader     // 剩余流（IdleReader 包装）
	model string
}

func newPeekStream(body io.ReadCloser, model string, idle time.Duration) *peekStream {
	// 先窥视 64KB 判 mock，再决定怎么包。
	head, err := io.ReadAll(io.LimitReader(body, 64<<10))
	_ = err // 窥视失败不致命：head 里有多少算多少，剩余流照常透传
	if isMockFrame(head) {
		// 读尽剩余（丢弃），提取错误文案。
		_, _ = io.Copy(io.Discard, body)
		body.Close()
		// 提取动作在 Read 时惰性完成 —— 不，这里直接构造错误流。
		msg := extractMockMsg(head)
		return &peekStream{
			pre:   bytes.NewReader(nil),
			rest:  errReader{&StreamError{Model: model, Code: "MOCK_FRAME", Msg: msg, kind: provider.ErrNotFound}},
			model: model,
		}
	}
	return &peekStream{
		pre:   bytes.NewReader(head),
		rest:  provider.NewIdleReader(body, idle),
		model: model,
	}
}

func (p *peekStream) Read(b []byte) (int, error) {
	if p.pre != nil {
		n, err := p.pre.Read(b)
		if err == io.EOF {
			p.pre = nil
			if p.rest == nil {
				return n, io.EOF
			}
			if n > 0 {
				return n, nil
			}
			return p.rest.Read(b)
		}
		return n, err
	}
	if p.rest == nil {
		return 0, io.EOF
	}
	return p.rest.Read(b)
}

func (p *peekStream) Close() error {
	if c, ok := p.rest.(io.Closer); ok {
		if _, isErr := p.rest.(errReader); !isErr {
			return c.Close()
		}
	}
	return nil
}

// errReader 把固定 error 包装成 io.Reader（mock 帧场景：流一开始就是错误）。
type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// isMockFrame 判据（实测事实 2）：id 前缀 chatcmpl-mock- 或 model=="mock"。
// 两个判据都查：上游形态可能变化，双保险。
func isMockFrame(head []byte) bool {
	s := string(head)
	return strings.Contains(s, "chatcmpl-mock-") || strings.Contains(s, `"model":"mock"`)
}

// extractMockMsg 从 mock 帧提取错误文案（delta.content 或 msg 字段）。
func extractMockMsg(head []byte) string {
	// 形态 1：SSE 帧 {"choices":[{"delta":{"content":"…"}}]}
	var frame struct {
		Choices []struct {
			Delta struct {
				Content string `json:"content"`
			} `json:"delta"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(head, &frame); err == nil && len(frame.Choices) > 0 && frame.Choices[0].Delta.Content != "" {
		return frame.Choices[0].Delta.Content
	}
	// 形态 2：非流式信封 {"code":"…","msg":"…"}
	var env struct {
		Msg string `json:"msg"`
	}
	if err := json.Unmarshal(head, &env); err == nil && env.Msg != "" {
		return env.Msg
	}
	return truncate(string(head), 256)
}

// sha256Sum 返回 hex 摘要（uid 生成用）。
func sha256Sum(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// ---------------------------------------------------------------------------
// Stream / Aggregate（provider.Upstream 接口方法）
// ---------------------------------------------------------------------------

// Stream 实现 provider.Upstream：逐帧透传上游 SSE，同时捕获流内 mock 错误帧
// 并转成可分类的 error（供 handler 冷却中招模型，R40 同款）。
func (c *Client) Stream(w http.ResponseWriter, r io.Reader, model string) (map[string]any, error) {
	var usage map[string]any
	var frame string

	serr := upstream.StreamWith(w, r,
		func(payload string) string {
			// hintFn 只在 error 帧上被调用；这里只取样、不附加 hint（返回空串 ⇒ 原文透传）。
			if frame == "" && !isLocalErrFrame(payload) {
				frame = payload
			}
			return ""
		},
		func(u map[string]any) { usage = u })

	if frame != "" {
		se := parseErrFrame(frame)
		if se != nil {
			se.Model = model
			log.Printf("comate: 流内 error 帧 uid=- model=%s code=%s msg=%.180s", model, se.Code, se.Msg)
			if serr == nil {
				return usage, se
			}
		}
	}
	return usage, serr
}

// Aggregate 实现 provider.Upstream（客户端 stream=false 时由 handler 调用）。
// 上游永远被请求 stream:true（不变量 1），聚合走流式帧；error 帧同样要识别
//（否则非流式客户端拿到空 choices 且不报错，R40 的第二个入口）。
// 响应 model 字段是短名（实测事实 3），回填客户端请求的原始模型名（R14 同款）。
func (c *Client) Aggregate(r io.Reader, model string) (map[string]any, error) {
	raw, err := io.ReadAll(io.LimitReader(r, 32<<20))
	if err != nil {
		return nil, err
	}
	se := scanAggregateErrFrame(raw)
	res, aerr := upstream.Aggregate(bytes.NewReader(raw))
	if res != nil {
		// 回填调用方请求的原始模型名（上游常回短名，R14 同款显式传参）。
		res["model"] = model
	}
	if se != nil {
		se.Model = model
		log.Printf("comate: 聚合流内 error 帧 model=%s code=%s msg=%.180s", model, se.Code, se.Msg)
		if aerr == nil {
			return res, se
		}
	}
	return res, aerr
}

// scanAggregateErrFrame 在已读尽的 SSE 文本里找第一帧**上游** error。
func scanAggregateErrFrame(raw []byte) *StreamError {
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" || payload == "[DONE]" {
			continue
		}
		if isLocalErrFrame(payload) {
			continue
		}
		if se := parseErrFrame(payload); se != nil {
			return se
		}
	}
	return nil
}

// isLocalErrFrame 判定「本地合成」的 error 帧（upstream 包在流尾注入的
// upstream_truncated / upstream_parse 等）——它们不是上游说的，不得二次归类。
func isLocalErrFrame(payload string) bool {
	for _, m := range localErrMarkers {
		if strings.Contains(payload, m) {
			return true
		}
	}
	return false
}

// localErrMarkers 本地合成帧的标记（与 cline 同款）。
var localErrMarkers = []string{
	"upstream_truncated", "upstream_parse", "empty upstream stream",
}

// parseErrFrame 把上游 error 帧解析成 StreamError。
// comate 的 error 帧形态：{"error":{"message":"…","code":"…"}}（OpenAI 兼容）。
func parseErrFrame(payload string) *StreamError {
	var f struct {
		Error struct {
			Message string `json:"message"`
			Code    string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(payload), &f); err != nil || f.Error.Message == "" {
		return nil
	}
	se := &StreamError{Code: f.Error.Code, Msg: f.Error.Message}
	if se.Code == "" {
		se.Code = "unknown"
	}
	// 文案分类（与 Classify 同一套 markers）。
	switch {
	case IsSessionDeadBody(f.Error.Message):
		se.kind = provider.ErrSessionDead
	case IsHardCreditBody(f.Error.Message):
		se.kind = provider.ErrHardCredit
	case IsModelUnavailableBody(f.Error.Message):
		se.kind = provider.ErrNotFound
	}
	return se
}
