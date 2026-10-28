// auto.go WPS Comate「自动获取登录态」流程。
//
// 背景：Comate 的凭据是浏览器 Cookie 里的 wps_sid，上游没有可编程登录接口
// （实测：Bearer / x-api-key / 查询参数一律 401，只认 Cookie）。手工路径
// （F12 → Application → Cookies → 复制粘贴）对普通用户门槛过高——粘错一个字符、
// 少复制半截、或者选中了 DevTools 表格里的列名，看到的都是同一句
// 「wps_sid 无效或已过期」，完全无从自查。故提供自动路径：
//
//	① 拉起一个**独立 profile** 的 Edge/Chrome（带 CDP 调试端口）
//	② 浏览器打开 WPS 登录页，用户在真实浏览器里正常登录（手机号/微信/扫码）
//	③ wild-work 经 CDP 轮询 Cookie，一旦 wps_sid 出现即捕获
//	④ 校验凭据有效（零 token：GET /models）→ 落盘 → 关闭浏览器、清理临时 profile
//
// 为什么用独立 profile 而不是用户日常 profile / InPrivate：
//   - 日常 profile 被浏览器独占锁（实测读不到 Cookie 库），且会污染用户登录态；
//   - 独立 profile 天然完全隔离，**多账号逐个添加互不干扰**——每次添加都是全新环境。
//
// ⚠️ 关键设计：「Cookie 出现」不等于「登录完成」。wps.cn 在未登录态也可能下发
// 一个 wps_sid（游客/预登录会话），上游对它的回答是 401 not_login。若一见到 Cookie
// 就收工，会把游客值落盘成账号，表现为「加进去了但用不了」。因此以**「/models 校验通过」**
// 为完成判据：轮询到 Cookie → 校验 → 只有校验通过才算成功，失败则继续等用户登录。
package logincomate

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"wild-work/internal/cdp"
)

// CookieName Comate 凭据所在的 Cookie 名。
const CookieName = "wps_sid"

// CookieDomain 匹配用域名片段。
const CookieDomain = "wps.cn"

// LoginURL 用户需要在浏览器里打开的地址（WPS 账号登录页）。
const LoginURL = "https://account.wps.cn/login"

// LoginTimeout 等待用户完成登录的最长时间。
const LoginTimeout = 10 * time.Minute

// pollInterval 轮询 Cookie 的间隔。
const pollInterval = 2 * time.Second

// Result 登录成功后的账号信息。
type Result struct {
	// UID 由 sid 的哈希推出（上游 /models 不回传身份字段）。
	UID string
	// Nickname 展示用昵称。
	Nickname string
	// Warn 同账号等非致命告警（空串表示无）。
	Warn string
}

// AutoSession 一次自动登录会话。
type AutoSession struct {
	browser *cdp.Browser
	cancel  context.CancelFunc
	mu      sync.Mutex
	status  string // pending / success / failed / cancelled
	err     error
	result  Result
}

// Status 当前状态。
func (s *AutoSession) Status() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status, s.err
}

// Result 成功时返回的账号信息。
func (s *AutoSession) Result() (Result, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status == "success" {
		return s.result, true
	}
	return Result{}, false
}

// setStatus 内部状态更新。
func (s *AutoSession) setStatus(status string, err error) {
	s.mu.Lock()
	s.status = status
	s.err = err
	s.mu.Unlock()
}

// AutoLogin 拉起浏览器并在后台等待登录完成。
//
// 返回的 AutoSession 立即可用（状态 pending）；调用方轮询 Status() / Result()。
// 登录成功后**自动关闭浏览器**（落盘由 onFound 负责，见下）。
//
// onFound 拿到捕获到的 wps_sid 后负责「校验 + 落盘 + 重载账号池」，返回 uid（校验失败
// 返回 error）。把落盘留在调用方（app 层）而不是这里，是为了与手工粘贴路径共用同一份
// 校验/同账号告警/日志纪律（日志不打凭据）——两条入口不该各写一份。
//
// onFound 返回 error 不视为致命：游客 Cookie、用户中途换账号都会校验失败，
// 此时继续轮询等真正的登录态。
func AutoLogin(onFound func(sid string) (Result, error)) (*AutoSession, error) {
	if err := checkGUI(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), LoginTimeout+2*time.Minute)

	browser, err := cdp.Launch(ctx, cdp.Options{StartURL: LoginURL})
	if err != nil {
		cancel()
		return nil, err
	}

	s := &AutoSession{browser: browser, cancel: cancel, status: "pending"}

	go func() {
		defer cancel()
		defer func() {
			if cerr := browser.Close(); cerr != nil {
				log.Printf("comate 自动登录：关闭浏览器失败: %v", cerr)
			}
		}()

		log.Printf("comate 自动登录：已拉起浏览器（端口 %d），等待用户在浏览器中登录 WPS…", browser.Port())

		deadline := time.Now().Add(LoginTimeout)
		var lastSID string
		var lastErr error
		sawRejected := false

		for time.Now().Before(deadline) {
			select {
			case <-ctx.Done():
				s.setStatus("cancelled", nil)
				log.Printf("comate 自动登录：已取消")
				return
			default:
			}

			sid, werr := currentSID(ctx, browser)
			if werr != nil {
				lastErr = werr
			} else if sid != "" && sid != lastSID {
				// Cookie 变了（首次出现或切换账号）→ 校验并落盘
				lastSID = sid
				r, ferr := onFound(sid)
				if ferr == nil {
					log.Printf("comate 自动登录：账号已添加 uid=%s 同账号告警=%v", r.UID, r.Warn != "")
					s.mu.Lock()
					s.result = r
					s.status = "success"
					s.err = nil
					s.mu.Unlock()
					return
				}
				// 校验失败：多为游客/未登录 Cookie，继续等用户登录
				lastErr = ferr
				if !sawRejected {
					sawRejected = true
					log.Printf("comate 自动登录：当前 Cookie 未通过校验（用户尚未登录），继续等待…")
				}
			}

			select {
			case <-ctx.Done():
				s.setStatus("cancelled", nil)
				return
			case <-time.After(pollInterval):
			}
		}

		// 超时：给出可读原因
		msg := "等待登录超时"
		if sawRejected {
			msg = "等待登录超时：检测到 Cookie，但未通过 Comate 校验。请确认已在弹出的浏览器窗口中完成 WPS 登录（该窗口需保持打开）"
		} else if lastErr != nil {
			msg = fmt.Sprintf("等待登录超时：%v", lastErr)
		}
		s.setStatus("failed", errors.New(msg))
		log.Printf("comate 自动登录：%s", msg)
	}()

	return s, nil
}

// GUIAvailable 探测本机能否真的拉起浏览器窗口。
//
// 三件事必须讲清楚，否则用户会以为按钮坏了：
//   1. 浏览器是**在运行 wild-work 的这台机器上**弹出来的，不是在你打开面板的电脑上。
//      NAS / docker / systemd 服务里跑 wild-work 时，那边没有桌面，窗口永远弹不出来。
//   2. Linux 上缺 DISPLAY/WAYLAND_DISPLAY 时 chromium 启动即崩，而 cdp.waitReady 仍要空转
//      30s 才报错 —— 用户看到的就是「点了没反应」。
//   3. 所以前端拿这个结果决定：能弹窗就以自动为主，弹不出来就直接展开手工方式。
//
// 返回 nil 表示可用；否则返回可直接展示给用户的解释。
func GUIAvailable() error {
	if runtime.GOOS != "linux" {
		return nil
	}
	if os.Getenv("DISPLAY") != "" || os.Getenv("WAYLAND_DISPLAY") != "" {
		return nil
	}
	return errors.New("wild-work 运行在没有桌面环境的服务器上（如 NAS），浏览器只会在运行 wild-work 的那台电脑上弹出，" +
		"不会出现在你现在的电脑上。请用下面的手工方式：复制一次 wps_sid 即可")
}

// checkGUI 是 GUIAvailable 的旧名，保留给启动路径做前置校验。
func checkGUI() error { return GUIAvailable() }

// currentSID 读一次当前 profile 的 wps_sid（无则空串）。
func currentSID(ctx context.Context, browser *cdp.Browser) (string, error) {
	cookies, err := browser.AllCookies(ctx, CookieDomain)
	if err != nil {
		return "", err
	}
	for _, c := range cookies {
		if c.Name == CookieName && c.Value != "" {
			return c.Value, nil
		}
	}
	return "", nil
}

// Cancel 取消自动登录并关闭浏览器。
func (s *AutoSession) Cancel() {
	s.setStatus("cancelled", nil)
	s.cancel()
}
