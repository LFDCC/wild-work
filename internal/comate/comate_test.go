package comate

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"wild-work/internal/auth"
	"wild-work/internal/provider"
)

// ---------------------------------------------------------------------------
// 真实 wire 形态（2026-10-27，经真实 wps_sid 直测 comate.wps.cn）
// ---------------------------------------------------------------------------

// mockFrame 是 comate 特有的「业务错误伪装成正常回答」形态：HTTP 200 + 一帧
// 看起来完全正常的 SSE，但 id 前缀 chatcmpl-mock-、model=="mock"。
// 判据必须卡死这两个字段（见 sse.go peekStream）。
const mockFrame = `data: {"id":"chatcmpl-mock-1793156969187","object":"chat.completion.chunk",` +
	`"model":"mock","choices":[{"index":0,"delta":{"content":"指定模型暂不可用，请切换其他模型后重试"},` +
	`"finish_reason":"stop"}],"usage":{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}}` +
	"\n\ndata: [DONE]\n\n"

// notLoginBody 是 401 的真实形态（bogus cookie 实测）。
const notLoginBody = `{"error":{"message":"未登录，请先登录","type":"authentication_error","code":"not_login"}}`

// quotaBody 是无效模型的非流式真实形态：**HTTP 200** + 业务错误信封（code 字段）。
const quotaBody = `{"code":"MULTIMODAL_TOKEN_QUOTA_EXCEEDED","data":"指定模型暂不可用，请切换其他模型后重试",` +
	`"msg":"指定模型暂不可用，请切换其他模型后重试"}`

// ---------------------------------------------------------------------------
// Classify：会话失效 → 积分耗尽 → 模型不可用 → 状态码
// ---------------------------------------------------------------------------

func TestClassify(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   provider.ErrKind
	}{
		{"401 not_login", 401, notLoginBody, provider.ErrSessionDead},
		{"401 登录校验失败", 401, `{"error":{"message":"登录校验失败","type":"authentication_error","code":"not_login"}}`, provider.ErrSessionDead},
		{"403", 403, `{"error":{"message":"forbidden"}}`, provider.ErrSessionDead},
		{"429", 429, `{"error":{"message":"too many requests"}}`, provider.ErrSoftRate},
		{"402 积分", 402, `{"error":{"message":"payment required"}}`, provider.ErrHardCredit},
		{"200+14018 积分耗尽", 200, `{"error":{"message":"Credits exhausted","code":14018}}`, provider.ErrHardCredit},
		{"200+积分不足中文", 200, `{"error":{"message":"积分不足，请前往 WPS 会员中心充值"}}`, provider.ErrHardCredit},
		{"200+指定模型暂不可用", 200, quotaBody, provider.ErrNotFound},
		{"404", 404, `{"error":{"message":"not found"}}`, provider.ErrNotFound},
		{"500", 500, `{"error":{"message":"internal"}}`, provider.ErrServer},
		{"502", 502, `bad gateway`, provider.ErrServer},
		{"未知形态透传", 400, `{"error":{"message":"bad request"}}`, provider.ErrClient},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Classify(tc.status, tc.body)
			if got != tc.want {
				t.Fatalf("Classify(%d, %q) = %v, want %v", tc.status, tc.body, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// mock 帧检测：peekStream 必须把 200+mock 帧转成 ErrNotFound 的 StreamError
// ---------------------------------------------------------------------------

func TestPeekStreamDetectsMockFrame(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(mockFrame))
	}))
	defer srv.Close()

	c := New()
	c.Base = srv.URL
	rc, status, _, err := c.ChatStream(fakeAuth(), chatBody("673940725/zhipu/glm-5.3-flash//public"))
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	defer rc.Close()
	if status != 200 {
		t.Fatalf("status = %d, want 200", status)
	}
	// 读流必须得到 StreamError（kind=ErrNotFound），而不是把 mock 文本当回答吐出去
	buf := make([]byte, 4096)
	n, rerr := rc.Read(buf)
	if rerr == nil {
		t.Fatalf("expected StreamError from mock frame, got content %q", buf[:n])
	}
	se, ok := rerr.(*StreamError)
	if !ok {
		t.Fatalf("read err = %T (%v), want *comate.StreamError", rerr, rerr)
	}
	if se.Kind() != provider.ErrNotFound {
		t.Fatalf("StreamError kind = %v, want ErrNotFound", se.Kind())
	}
	if !strings.Contains(se.Error(), "指定模型暂不可用") {
		t.Fatalf("StreamError msg = %q, want 含「指定模型暂不可用」", se.Error())
	}
}

// 正常流不得被 peekStream 误杀：首帧照常透传。
func TestPeekStreamPassesNormalStream(t *testing.T) {
	const normalFrame = `data: {"id":"chatcmpl-real-1","model":"glm-5.3-flash",` +
		`"choices":[{"index":0,"delta":{"content":"你好"},"finish_reason":null}]}` + "\n\n" +
		`data: [DONE]` + "\n\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(normalFrame))
	}))
	defer srv.Close()

	c := New()
	c.Base = srv.URL
	rc, status, _, err := c.ChatStream(fakeAuth(), chatBody("673940725/zhipu/glm-5.3-flash//public"))
	if err != nil || status != 200 {
		t.Fatalf("ChatStream: err=%v status=%d", err, status)
	}
	defer rc.Close()
	buf := make([]byte, 4096)
	n, rerr := rc.Read(buf)
	if rerr != nil {
		t.Fatalf("normal stream read: %v", rerr)
	}
	if !strings.Contains(string(buf[:n]), "chatcmpl-real-1") {
		t.Fatalf("first chunk lost: %q", buf[:n])
	}
}

// ---------------------------------------------------------------------------
// 鉴权形态：Cookie-only（Bearer 无效）—— applyHeaders 只设 Cookie
// ---------------------------------------------------------------------------

func TestApplyHeadersCookieOnly(t *testing.T) {
	var gotCookie, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCookie = r.Header.Get("Cookie")
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"code":0,"msg":"","data":{"items":[]}}`))
	}))
	defer srv.Close()

	c := New()
	c.Base = srv.URL
	_, _ = c.getJSON(fakeAuth(), EpModels)
	if gotCookie != CookieName+"="+fakeSID {
		t.Fatalf("Cookie = %q, want %q", gotCookie, CookieName+"="+fakeSID)
	}
	if gotAuth != "" {
		t.Fatalf("Authorization must be empty (comate rejects Bearer), got %q", gotAuth)
	}
}

// ---------------------------------------------------------------------------
// FetchModels：items 原样透传 + 能力字段填充（ContextFromAPI=true）
// ---------------------------------------------------------------------------

func TestFetchModels(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"msg":"","data":{"auto":{},"items":[` +
			`{"display_name":"GLM-5.3 Flash","name":"673940725/zhipu/glm-5.3-flash//public",` +
			`"llm_types":["llm-chat"],"context_window":1000000,"support_thinking":true,` +
			`"thinking_levels":["low","high"],"model_tier":"flash","consume_ratio":0.04},` +
			`{"display_name":"DeepSeek V4.1 Flash","name":"673940725/deepseek/deepseek-v4.1-flash//public",` +
			`"llm_types":["llm-multimodal"],"context_window":1000000,"support_thinking":true,` +
			`"thinking_levels":null,"model_tier":"flash","consume_ratio":0.05}` +
			`]}}`))
	}))
	defer srv.Close()

	c := New()
	c.Base = srv.URL
	mis, err := c.FetchModels(fakeAuth())
	if err != nil {
		t.Fatalf("FetchModels: %v", err)
	}
	if len(mis) != 2 {
		t.Fatalf("models = %d, want 2", len(mis))
	}
	// 排序是字典序：deepseek < zhipu
	glm := mis[1]
	if glm.ID != "673940725/zhipu/glm-5.3-flash//public" {
		t.Fatalf("ID = %q（不得改名）", glm.ID)
	}
	if glm.Name != "GLM-5.3 Flash" {
		t.Fatalf("Name = %q", glm.Name)
	}
	if glm.ContextWindow != 1000000 || !glm.ContextFromAPI {
		t.Fatalf("ContextWindow = %d ContextFromAPI = %v", glm.ContextWindow, glm.ContextFromAPI)
	}
	if !glm.SupportsReasoning {
		t.Fatal("SupportsReasoning 应为 true")
	}
	if len(glm.ReasoningEfforts) != 2 || glm.ReasoningEfforts[0] != "low" {
		t.Fatalf("ReasoningEfforts = %v", glm.ReasoningEfforts)
	}
	ds := mis[0]
	if ds.ID != "673940725/deepseek/deepseek-v4.1-flash//public" {
		t.Fatalf("ds ID = %q", ds.ID)
	}
	if !ds.SupportsImages {
		t.Fatal("llm-multimodal 应置 SupportsImages")
	}
	// knownIDs 已记住：目录拉取失败时兜底返回缓存
	c.Base = srv.URL + "/broken"
	cached, err := c.FetchModels(fakeAuth())
	if err != nil || len(cached) != 2 {
		t.Fatalf("缓存兜底失败: err=%v n=%d", err, len(cached))
	}
}

// ---------------------------------------------------------------------------
// ValidateSID：零 token 验证 + 401 人话错误
// ---------------------------------------------------------------------------

func TestValidateSID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Cookie") != CookieName+"="+fakeSID {
			w.WriteHeader(401)
			_, _ = w.Write([]byte(notLoginBody))
			return
		}
		_, _ = w.Write([]byte(`{"code":0,"msg":"","data":{"items":[]}}`))
	}))
	defer srv.Close()

	c := New()
	c.Base = srv.URL
	uid, err := c.validateSID(fakeSID)
	if err != nil {
		t.Fatalf("ValidateSID: %v", err)
	}
	if uid == "" || !strings.HasPrefix(uid, "sid-") {
		t.Fatalf("uid = %q, want sid-<sha256[:16]>", uid)
	}
	// 同 sid 稳定
	uid2, _ := c.validateSID(fakeSID)
	if uid2 != uid {
		t.Fatalf("uid 不稳定: %q vs %q", uid, uid2)
	}
	// bogus sid → 人话错误（不是裸 HTTP 401）
	if _, err := c.validateSID("bogus"); err == nil || !strings.Contains(err.Error(), "无效") {
		t.Fatalf("bogus sid err = %v, want 含「无效」的人话错误", err)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

const fakeSID = "V02FakeSidForUnitTests0000000000000000000000000001"

func fakeAuth() *auth.Auth {
	return &auth.Auth{Kind: "comate", UID: "sid-test", AccessToken: fakeSID}
}

func chatBody(model string) []byte {
	b, _ := json.Marshal(map[string]any{
		"model":  model,
		"stream": true,
		"messages": []map[string]any{
			{"role": "user", "content": "hi"},
		},
	})
	return b
}

// ---------------------------------------------------------------------------
// 2026-10-28 修复回归：peekStream 必须增量判定（不得攒批）+ prettifyID 单段兜底
// ---------------------------------------------------------------------------

// 首帧到达即交付：旧实现用 io.ReadAll(LimitReader(body, 64KB))，要等读满 64KB 或 EOF，
// 实测首帧 flush 后上游停 1.5s，客户端首次 Read 也被推迟 1.5s（所有对话退化成
// 「正文突然整段出现」）。此处断言：上游写出首帧后阻塞 1s，客户端必须立刻拿到。
func TestPeekStreamDeliversFirstFrameWithoutWaiting(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"id":"chatcmpl-real-1","choices":[{"delta":{"content":"早"}}]}` + "\n\n"))
		w.(http.Flusher).Flush()
		<-release // 首帧之后一直不发（模拟慢思考）
	}))
	defer srv.Close()
	defer close(release)

	c := New()
	c.Base = srv.URL
	rc, status, _, err := c.ChatStream(fakeAuth(), chatBody("673940725/zhipu/glm-5.3-flash//public"))
	if err != nil || status != 200 {
		t.Fatalf("ChatStream: err=%v status=%d", err, status)
	}
	defer rc.Close()

	start := time.Now()
	buf := make([]byte, 4096)
	n, rerr := rc.Read(buf)
	elapsed := time.Since(start)
	if rerr != nil {
		t.Fatalf("read: %v", rerr)
	}
	if !strings.Contains(string(buf[:n]), "chatcmpl-real-1") {
		t.Fatalf("首帧丢失: %q", buf[:n])
	}
	if elapsed > 400*time.Millisecond {
		t.Fatalf("首帧交付延迟 %v（应≈0，旧实现会等到 1s 后的 release）", elapsed)
	}
}

// 首字节前的空闲必须有兜底：旧实现 IdleReader 包在窥视之后，上游 200 后不发字节会永久挂住。
func TestPeekStreamIdleGuardCoversBeforeFirstByte(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-release // 一个字节都不发
	}))
	defer srv.Close()
	defer close(release)

	c := New()
	c.Base = srv.URL
	c.IdleTimeout = 200 * time.Millisecond
	rc, _, _, err := c.ChatStream(fakeAuth(), chatBody("673940725/zhipu/glm-5.3-flash//public"))
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	defer rc.Close()

	start := time.Now()
	buf := make([]byte, 64)
	if _, rerr := rc.Read(buf); rerr == nil {
		t.Fatal("上游不发字节应报空闲超时，实际读成功")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("空闲兜底失效：等了 %v", elapsed)
	}
}

// 单段 ID（无 "/"）不得 panic：旧实现 parts[len(parts)-2] 越界。
// 触发路径：上游 display_name 为空且 name 无斜杠 ⇒ 整个 FetchModels 崩。
func TestPrettifyIDSingleSegment(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"glm-5.3-flash", "Glm 5.3 flash"},
		{"673940725/zhipu/glm-5.3-flash//public", "Glm 5.3 flash"},
		{"", ""},
		{"//", ""},
	} {
		if got := prettifyID(tc.in); got != tc.want {
			t.Fatalf("prettifyID(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// display_name 为空 + 单段 name ⇒ FetchModels 不得崩（旧实现此处 panic）。
func TestFetchModelsEmptyDisplayNameSingleSegment(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"data":{"items":[{"display_name":"","name":"glm-5.3-flash"}]}}`))
	}))
	defer srv.Close()
	c := New()
	c.Base = srv.URL
	mis, err := c.FetchModels(fakeAuth())
	if err != nil || len(mis) != 1 {
		t.Fatalf("FetchModels: err=%v n=%d", err, len(mis))
	}
	if mis[0].ID != "glm-5.3-flash" {
		t.Fatalf("ID = %q（不得改名）", mis[0].ID)
	}
	if mis[0].Name == "" {
		t.Fatal("display_name 为空时应由 prettifyID 兜底出展示名")
	}
}
