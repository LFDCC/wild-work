// login_test.go 锁定扫码登录/浏览器接管的**纯逻辑**部分（不触网）。
//
// 网络路径（qr.wps.cn 三个接口、CDP 接管）在真实环境验证过，单测只覆盖那些
// 「写错了不会报错、只会静默出错」的地方：JSONP 包装的剥离、Cookie 里挑 sid、
// 会话状态机对外的可见性（面板轮询就读这些字段）。
package logincomate

import (
	"errors"
	"testing"

	"wild-work/internal/cdp"
)

// TestJSONPBodyStripsWrapper 上游一律回 JSONP（`cb({...})`），
// 少剥一层会让 json.Unmarshal 失败、面板永远停在「正在申请二维码」。
func TestJSONPBodyStripsWrapper(t *testing.T) {
	cases := []struct{ in, want string }{
		{`cb({"state":"pending"})`, `{"state":"pending"}`},
		{`cb ({"state":"pending"})`, `{"state":"pending"}`},
		{`  cb({"state":"pending"});  `, `{"state":"pending"}`},
		{`{"state":"pending"}`, `{"state":"pending"}`}, // 上游偶尔回裸 JSON
		{``, ``},
	}
	for _, c := range cases {
		if got := jsonpBody(c.in); got != c.want {
			t.Errorf("jsonpBody(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestSIDOfPicksWpsSid Cookie 列表里可能有十几个 Cookie，只能取 wps_sid；
// 取错一个（如 kso_sid）就等于把别的会话凭据写进账号文件。
func TestSIDOfPicksWpsSid(t *testing.T) {
	cookies := []cdp.Cookie{
		{Name: "kso_sid", Value: "wrong"},
		{Name: "wps_sid", Value: "right"},
		{Name: "wps_sid_extra", Value: "also-wrong"},
	}
	if got := sidOf(cookies); got != "right" {
		t.Errorf("应取 wps_sid，得到 %q", got)
	}
	if got := sidOf(nil); got != "" {
		t.Errorf("无 Cookie 时应为空，得到 %q", got)
	}
	// 空值不得当作凭据（否则会拿空 sid 去校验并在面板报「sid 无效」）
	if got := sidOf([]cdp.Cookie{{Name: "wps_sid", Value: ""}}); got != "" {
		t.Errorf("空值 wps_sid 不应作为凭据，得到 %q", got)
	}
}

// TestQRSessionSnapshotStates 面板靠 Snapshot 的 state 文案驱动 UI，
// 状态机要保证：初始 pending、失败带 error、取消后不再回报 pending。
func TestQRSessionSnapshotStates(t *testing.T) {
	s := &QRSession{cancel: func() {}}
	s.set(QRStatePending, nil, "用手机 WPS 扫码")
	snap := s.Snapshot()
	if snap["state"] != QRStatePending {
		t.Fatalf("初始状态应为 %q，得到 %v", QRStatePending, snap["state"])
	}
	if _, ok := snap["error"]; ok {
		t.Error("pending 时不应带 error 字段（前端会当成失败）")
	}

	s.set(QRStateFailed, errors.New("二维码申请失败"), "")
	snap = s.Snapshot()
	if snap["state"] != QRStateFailed {
		t.Fatalf("应为 failed，得到 %v", snap["state"])
	}
	if snap["error"] != "二维码申请失败" {
		t.Errorf("error 文案应透传，得到 %v", snap["error"])
	}

	s.set(QRStateCancelled, nil, "")
	if snap := s.Snapshot(); snap["state"] != QRStateCancelled {
		t.Errorf("取消后应为 cancelled，得到 %v", snap["state"])
	}
}

// TestQRSessionSuccessCarriesAccount 成功后必须回传 uid/nickname，
// 面板据此提示「已添加哪个账号」；丢了这些字段用户不知道加的是谁。
func TestQRSessionSuccessCarriesAccount(t *testing.T) {
	s := &QRSession{cancel: func() {}}
	s.succeed(Result{UID: "sid-abc123", Nickname: "WPS 账号 abc123", Warn: "该 sid 已存在"})
	snap := s.Snapshot()
	if snap["state"] != QRStateSuccess {
		t.Fatalf("应为 success，得到 %v", snap["state"])
	}
	if snap["uid"] != "sid-abc123" || snap["nickname"] != "WPS 账号 abc123" {
		t.Errorf("成功快照缺账号信息: %+v", snap)
	}
	if snap["warning"] != "该 sid 已存在" {
		t.Errorf("非致命告警应透传，得到 %v", snap["warning"])
	}
}
