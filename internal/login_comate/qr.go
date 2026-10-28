// qr.go WPS Comate「扫码登录」——全程服务端 HTTP，不需要桌面、不需要浏览器。
//
// 与 auto.go 的 CDP 路径相比，这条路解决的是**部署环境问题**：CDP 只能拉起
// 「运行 wild-work 的那台机器」的浏览器。NAS / docker / systemd 里没有桌面，
// 窗口永远弹不出来；而用户自己浏览器里的 wps_sid 又因为同源策略读不到。
// 扫码登录把「拿凭据」这件事整个搬到服务端：
//
//	① 服务端向 qr.wps.cn 申请一个登录二维码（loginid）
//	② 面板把二维码图片显示出来，用户用手机 WPS Office 扫
//	③ 服务端长轮询扫码状态，手机确认后拿到一次性 authcode
//	④ authcode 换 session Cookie，捞到 wps_sid → 校验 → 落盘
//
// 这条流程与 WPS 网页版登录页（account.wps.cn/login）用的是同一套接口，
// 区别只是网页版由浏览器 JS 完成 ④，这里由服务端完成。
//
// 拼图是实测出来的：`GET /api/v3/login_qrcode` 出 loginid，
// `GET /api/v3/login_qrcode/url` 出二维码图，`GET /api/v3/login_qrcode/login`
// 长轮询出 state（pending → scan → logined）/authcode（见 docs/决议项.md R132）。
package logincomate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"sync"
	"time"
)

// CookieName Comate 凭据所在的 Cookie 名。
const CookieName = "wps_sid"

// Result 登录成功后的账号信息。
type Result struct {
	// UID 由 sid 的哈希推出（上游 /models 不回传身份字段）。
	UID string
	// Nickname 展示用昵称。
	Nickname string
	// Warn 同账号等非致命告警（空串表示无）。
	Warn string
}

const (
	// qrAPIBase 二维码服务（与 account.wps.cn 分离的独立域）。
	qrAPIBase = "https://qr.wps.cn"
	// accountBase WPS 账号域：换 session Cookie 在这里。
	accountBase = "https://account.wps.cn"
	// qrTimeout 整条扫码流程的最长等待（二维码过期会自动换新）。
	qrTimeout = 5 * time.Minute
	// qrPollTimeout 单次长轮询的 HTTP 超时（上游会挂起到用户扫码，给足余量）。
	qrPollTimeout = 45 * time.Second
	// qrRefreshGap 二维码主动换新间隔（上游 TTL 约 2 分钟，留出余量）。
	qrRefreshGap = 110 * time.Second
)

// 扫码会话状态（前端据此渲染）。
const (
	QRStatePending   = "pending"   // 二维码已就绪，等待扫码
	QRStateScanned   = "scanned"   // 已扫码，等待手机确认
	QRStateSuccess   = "success"   // 已拿到 wps_sid 并落盘
	QRStateFailed    = "failed"    // 失败（含超时）
	QRStateCancelled = "cancelled" // 用户取消
)

// QRSession 一次扫码登录会话。
type QRSession struct {
	cancel context.CancelFunc

	mu     sync.Mutex
	state  string
	qrURL  string
	err    error
	hint   string
	result Result
}

// Snapshot 供 HTTP 状态接口序列化。
func (s *QRSession) Snapshot() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]any{"state": s.state, "qr": s.qrURL}
	if s.err != nil {
		out["error"] = s.err.Error()
	}
	if s.hint != "" {
		out["hint"] = s.hint
	}
	if s.state == QRStateSuccess {
		out["uid"] = s.result.UID
		out["nickname"] = s.result.Nickname
		if s.result.Warn != "" {
			out["warning"] = s.result.Warn
		}
	}
	return out
}

// State 当前状态。
func (s *QRSession) State() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// Cancel 取消会话。
func (s *QRSession) Cancel() { s.set(QRStateCancelled, nil, "") }

func (s *QRSession) set(state string, err error, hint string) {
	s.mu.Lock()
	s.state = state
	s.err = err
	s.hint = hint
	s.mu.Unlock()

	s.cancel()
}

func (s *QRSession) setQR(u string) {
	s.mu.Lock()
	s.qrURL = u
	s.mu.Unlock()
}

func (s *QRSession) succeed(r Result) {
	s.mu.Lock()
	s.result = r
	s.state = QRStateSuccess
	s.err = nil
	s.hint = ""
	s.mu.Unlock()
}

// StartQR 申请二维码并在后台推进扫码流程。
//
// onFound 与手工粘贴路径共用：拿到 wps_sid 后校验 + 落盘 + 重载账号池，
// 返回 error 表示这个 sid 不可用（不该发生，扫码拿到的就是登录态）。
func StartQR(onFound func(sid string) (Result, error)) (*QRSession, error) {
	ctx, cancel := context.WithTimeout(context.Background(), qrTimeout+30*time.Second)
	s := &QRSession{cancel: cancel, state: QRStatePending}

	jar, err := cookiejar.New(nil)
	if err != nil {
		cancel()
		return nil, err
	}
	cli := &http.Client{Jar: jar}

	loginID, qrURL, err := newQRLogin(ctx, cli)
	if err != nil {
		cancel()
		return nil, err
	}
	s.setQR(qrURL)
	log.Printf("comate 扫码登录：二维码已生成 loginid=%s", loginID)

	go s.run(ctx, cli, loginID, onFound)
	return s, nil
}

// run 长轮询扫码状态。
func (s *QRSession) run(ctx context.Context, cli *http.Client, loginID string, onFound func(string) (Result, error)) {
	defer s.cancel()

	state := "scan"
	refreshed := time.Now()
	lastErr := ""

	for {
		select {
		case <-ctx.Done():
			if s.State() == QRStatePending || s.State() == QRStateScanned || s.State() == QRStateCancelled {
				s.set(QRStateFailed, errors.New("扫码登录超时（5 分钟）：二维码已失效，请重新获取"), "")
			}
			return
		default:
		}

		// 主动换新二维码：上游 TTL 约 2 分钟。
		if time.Since(refreshed) > qrRefreshGap {
			if id, u, err := newQRLogin(ctx, cli); err == nil {
				loginID, refreshed = id, time.Now()
				state = "scan"
				s.setQR(u)
				log.Printf("comate 扫码登录：二维码已刷新 loginid=%s", id)
			}
		}

		resp, err := pollQR(ctx, cli, loginID, state)
		if err != nil {
			if ctx.Err() != nil {
				continue
			}
			msg := err.Error()
			if msg != lastErr {
				lastErr = msg
				log.Printf("comate 扫码登录：轮询失败: %v", err)
			}
			time.Sleep(time.Second)
			continue
		}

		if resp.Result != "" && resp.Result != "ok" {
			// QRCodeExpired / InvalidQRLoginId：换一张继续等。
			if id, u, err := newQRLogin(ctx, cli); err == nil {
				loginID, refreshed = id, time.Now()
				state = "scan"
				s.setQR(u)
				log.Printf("comate 扫码登录：二维码过期，已换新 loginid=%s", id)
				continue
			}
			s.set(QRStateFailed, fmt.Errorf("获取登录二维码失败：%s", resp.Result), "")
			return
		}

		switch resp.State {
		case "scan":
			// 已扫码：切到 confirm 继续轮询。
			if s.State() != QRStateScanned {
				s.mu.Lock()
				s.state = QRStateScanned
				s.hint = "已扫码，请在手机 WPS 上确认登录"
				s.mu.Unlock()
				log.Printf("comate 扫码登录：已扫码（nickname=%s），等待手机确认", resp.Nickname)
			}
			state = "confirm"
		case "logined":
			sid, xerr := exchangeSession(ctx, cli, resp)
			if xerr != nil {
				s.set(QRStateFailed, xerr, "")
				log.Printf("comate 扫码登录：%v", xerr)
				return
			}
			r, ferr := onFound(sid)
			if ferr != nil {
				s.set(QRStateFailed, ferr, "")
				log.Printf("comate 扫码登录：账号落盘失败: %v", ferr)
				return
			}
			s.succeed(r)
			log.Printf("comate 扫码登录：成功 uid=%s", r.UID)
			return
		}
	}
}

// qrLoginResp 长轮询响应（字段按实测裁剪）。
type qrLoginResp struct {
	Result      string `json:"result"`
	State       string `json:"state"`
	Msg         string `json:"msg"`
	Nickname    string `json:"nickname"`
	Avatar      string `json:"avatar"`
	AuthCode    string `json:"authcode"`
	KsoAuthCode string `json:"kso_authcode"`
}

// newQRLogin 申请一个二维码，返回 loginid 与图片地址。
func newQRLogin(ctx context.Context, cli *http.Client) (string, string, error) {
	raw, err := getText(ctx, cli, qrAPIBase+"/api/v3/login_qrcode?_jsonp=cb", accountBase+"/")
	if err != nil {
		return "", "", err
	}
	var first struct {
		LoginID string `json:"loginid"`
	}
	if err := json.Unmarshal([]byte(jsonpBody(raw)), &first); err != nil || first.LoginID == "" {
		return "", "", fmt.Errorf("申请登录二维码失败：响应无法解析（%s）", truncateStr(raw, 120))
	}

	q := url.Values{}
	q.Set("loginid", first.LoginID)
	q.Set("data", "{}")
	raw, err = getText(ctx, cli, qrAPIBase+"/api/v3/login_qrcode/url?"+q.Encode(), accountBase+"/")
	if err != nil {
		return "", "", err
	}
	var second struct {
		Result string `json:"result"`
		URL    string `json:"url"`
	}
	if err := json.Unmarshal([]byte(jsonpBody(raw)), &second); err != nil || second.URL == "" {
		return "", "", fmt.Errorf("获取二维码图片失败：响应无法解析（%s）", truncateStr(raw, 120))
	}
	return first.LoginID, second.URL, nil
}

// pollQR 长轮询一次扫码状态。
func pollQR(ctx context.Context, cli *http.Client, loginID, state string) (qrLoginResp, error) {
	q := url.Values{}
	q.Set("loginid", loginID)
	q.Set("state", state)
	q.Set("_jsonp", "cb")
	cctx, cancel := context.WithTimeout(ctx, qrPollTimeout)
	defer cancel()
	raw, err := getText(cctx, cli, qrAPIBase+"/api/v3/login_qrcode/login?"+q.Encode(), accountBase+"/")
	if err != nil {
		return qrLoginResp{}, err
	}
	var resp qrLoginResp
	if err := json.Unmarshal([]byte(jsonpBody(raw)), &resp); err != nil {
		return qrLoginResp{}, fmt.Errorf("扫码状态响应无法解析（%s）", truncateStr(raw, 120))
	}
	return resp, nil
}

// exchangeSession 用一次性 authcode 换取会话 Cookie，返回 wps_sid。
//
// 网页版登录页在拿到 authcode 后按客户端类型分岔（WPS 客户端 / kso 客户端 / 普通网页），
// 这里按「普通网页」的顺序依次尝试，任一步拿到 wps_sid 即成功：
//
//	① POST /api/session/exchange/login   {authcode}       —— 网页版主路径
//	② GET  /loginCallbackApp?authcode=…                   —— 兼容路径（会 302 后下发 Cookie）
//	③ POST /passport/secure/api/grant_token {kso_authcode} —— kso 客户端码
//
// 三条都走不通时给出可读错误，而不是把 authcode 抛给用户。
func exchangeSession(ctx context.Context, cli *http.Client, resp qrLoginResp) (string, error) {
	jctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if resp.AuthCode != "" {
		body, _ := json.Marshal(map[string]string{"authcode": resp.AuthCode})
		if _, err := postJSON(jctx, cli, accountBase+"/api/session/exchange/login", body); err != nil {
			log.Printf("comate 扫码登录：exchange/login 失败: %v", err)
		}
		if sid := cookieValue(cli, CookieName); sid != "" {
			return sid, nil
		}
		// 兼容路径：整页导航（跟随 302）。
		q := url.Values{}
		q.Set("authcode", resp.AuthCode)
		q.Set("logintype", "qr")
		if _, err := getText(jctx, cli, accountBase+"/loginCallbackApp?"+q.Encode(), accountBase+"/login"); err != nil {
			log.Printf("comate 扫码登录：loginCallbackApp 失败: %v", err)
		}
		if sid := cookieValue(cli, CookieName); sid != "" {
			return sid, nil
		}
	}

	if resp.KsoAuthCode != "" {
		body, _ := json.Marshal(map[string]any{
			"grant_type":     "authorization_code",
			"code":           resp.KsoAuthCode,
			"login_scenario": "qr",
			"is_append":      true,
		})
		if _, err := postJSON(jctx, cli, accountBase+"/passport/secure/api/grant_token", body); err != nil {
			log.Printf("comate 扫码登录：grant_token 失败: %v", err)
		}
		if sid := cookieValue(cli, CookieName); sid != "" {
			return sid, nil
		}
	}

	return "", errors.New("扫码已确认，但换取登录态失败（上游未下发 wps_sid）。请改用「手工方式」粘贴一次")
}

// cookieValue 从会话 Cookie 罐里取指定 Cookie 的值。
func cookieValue(cli *http.Client, name string) string {
	if cli.Jar == nil {
		return ""
	}
	for _, u := range []string{accountBase + "/", "https://www.wps.cn/", qrAPIBase + "/"} {
		parsed, err := url.Parse(u)
		if err != nil {
			continue
		}
		for _, c := range cli.Jar.Cookies(parsed) {
			if c.Name == name && c.Value != "" {
				return c.Value
			}
		}
	}
	return ""
}

// getText 发一个 GET 并读尽响应体（自动跟随跳转）。
func getText(ctx context.Context, cli *http.Client, rawURL, referer string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("Referer", referer)
	req.Header.Set("Accept", "*/*")
	resp, err := cli.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 256<<10))
	return string(body), nil
}

// postJSON 发一个 JSON POST（失败只回错误，不关心响应体）。
func postJSON(ctx context.Context, cli *http.Client, rawURL string, payload []byte) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, strings.NewReader(string(payload)))
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("Referer", accountBase+"/login")
	req.Header.Set("Content-Type", "application/json")
	resp, err := cli.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	return string(body), nil
}

// jsonpBody 剥掉 `cb(...)` 外壳；不是 JSONP 形态则原样返回。
func jsonpBody(s string) string {
	s = strings.TrimSpace(s)
	i := strings.Index(s, "(")
	j := strings.LastIndex(s, ")")
	if i > 0 && j > i && strings.HasSuffix(strings.TrimSpace(s[:i]), "cb") {
		return s[i+1 : j]
	}
	return s
}

// browserUA 与网页版登录页一致的 UA（上游对空 UA 会走不同的分支）。
const browserUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36"

// truncateStr 截断用于错误信息。
func truncateStr(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
