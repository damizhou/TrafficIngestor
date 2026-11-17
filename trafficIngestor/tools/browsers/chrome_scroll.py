"""Chrome 逐段下滑采集后端（chrome.py 的副本流程）。

驱动创建、诊断与进程清理复用 chrome.py，只替换访问流程：

    打开网页 → 初始等待 6 秒并处理弹窗 → 保存初始快照
    → 循环：读取 scrollY / innerHeight / 页面总高度，未到底则平滑下滑
      “当前页面总高度的 1/3”，再等待 5 秒（期间继续处理弹窗）
    → 确认到底后额外等待 10 秒
    → 保存最终快照、当前窗口截图、HTML/文本（尽力收尾）

所有时间相对抓包启动时刻（capture_started_at）计算，单次访问总预算约 120 秒，
各边界见下方 *_SECS 常量。抓包参数、尾段与停止方式通过 CAPTURE_PROFILE 交给
独立测试入口 traffic_capture_scroll_test/run_capture.py 使用；本模块未接入
现有 single_csv / BaseAction 流程。
"""

import json
import os
import re
import signal
import threading
import time
from urllib.parse import urlparse

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By

try:
    from tools.browsers import chrome as chrome_backend
except ImportError:  # 直接以脚本方式加载
    import chrome as chrome_backend

# ---- 对外复用 chrome.py 的接口 ----
build_browser_error_diagnostics = chrome_backend.build_browser_error_diagnostics
get_main_document_response = chrome_backend.get_main_document_response
add_cookies = chrome_backend.add_cookies

_log_warning = chrome_backend._log_warning

# ---- 时间预算（秒，均相对抓包启动时刻）----
INITIAL_WAIT_SECS = 6
SCROLL_WAIT_SECS = 5
BOTTOM_WAIT_SECS = 10
PAGE_LOAD_TIMEOUT_SECS = 25        # 导航最长等待
NAVIGATION_DEADLINE_SECS = 30      # 导航等待不越过采集开始后约 30 秒
SCRIPT_TIMEOUT_SECS = 10
SCROLL_LOOP_DEADLINE_SECS = 90     # 不再启动新的滚动循环
PAUSE_DEADLINE_SECS = 105          # pause() 不再主动继续等待
BUDGET_SIGNAL_SECS = 106           # 浏览器侧预算信号，流程尽快收尾
FORCE_CLEANUP_SECS = 113           # 仍未结束则强制清理浏览器进程
CAPTURE_TAIL_SECS = 5              # 正常情况下保留的抓包尾段
CAPTURE_TAIL_DEADLINE_SECS = 118   # 尾段等待截止边界
CAPTURE_BUDGET_SECS = 120          # 停止抓包的目标总预算

SCROLL_FRACTION = 1 / 3            # 每次下滑当前页面总高度的 1/3
BOTTOM_TOLERANCE_PX = 5
POPUP_SCAN_INTERVAL_SECS = 1.0
MAX_CONSENT_IFRAMES = 3
MIN_SCREENSHOT_TIMEOUT_SECS = 3

# run_capture.py 读取的抓包配置：tcpdump -i eth0 -U -s 0，SIGINT 停止。
CAPTURE_PROFILE = {
    "tcpdump_interface": "eth0",
    "tcpdump_extra_args": ("-U", "-s", "0"),
    "tail_secs": CAPTURE_TAIL_SECS,
    "tail_deadline_secs": CAPTURE_TAIL_DEADLINE_SECS,
    "budget_secs": CAPTURE_BUDGET_SECS,
    "stop_signal": signal.SIGINT,
}

# 站点 Cookie 文件：{"example.com": "/app/cookies/example.json"}。
# 仅当访问站点命中此表时才导入 Cookie 并刷新页面；默认为空，不进入该分支。
COOKIE_FILES = {}


# 读取页面滚动状态与图片加载情况（只观察，不触发补加载）。
JS_PAGE_SNAPSHOT = r"""
const body = document.body;
const doc = document.documentElement;
const totalHeight = Math.max(
  body ? body.scrollHeight : 0,
  doc ? doc.scrollHeight : 0
);
const imgs = Array.from(document.images || []);
let complete = 0, broken = 0;
for (const img of imgs) {
  if (img.complete) {
    complete += 1;
    if (!img.naturalWidth) broken += 1;
  }
}
return {
  url: window.location.href,
  title: document.title || '',
  readyState: document.readyState,
  scrollY: Math.round(window.scrollY || window.pageYOffset || 0),
  innerHeight: Math.round(window.innerHeight || 0),
  totalHeight: Math.round(totalHeight),
  images: {total: imgs.length, complete: complete, broken: broken,
           pending: imgs.length - complete},
};
"""

JS_SMOOTH_SCROLL_BY = r"""
window.scrollBy({top: arguments[0], left: 0, behavior: 'smooth'});
"""

# 弹窗处理：只点击可见、未被遮挡、可操作的站点按钮，不删除遮罩。
# 优先级：首次提示的 Accept → 后续隐私选择中的 Reject/Opt-out → 关闭按钮。
# 主页面只处理位于弹层上下文（fixed/sticky、dialog、consent/cookie/modal 等容器）
# 内的按钮；consent iframe 内整页都视为弹层上下文。
JS_HANDLE_POPUPS = r"""
const inConsentFrame = !!arguments[0];
const MARK = 'data-ti-popup-clicked';
const ACCEPT = /^(accept|accept all|accept all cookies|accept cookies|accept and close|accept & close|accept and continue|accept & continue|i accept|agree|i agree|agree and close|agree & close|agree and continue|agree & continue|allow|allow all|allow all cookies|allow cookies|got it|ok|okay|yes, i agree|yes, i'm happy|i'm ok with that|consent|同意|接受|全部接受|接受全部|我同意|同意并继续|知道了|好的)$/i;
const REJECT = /^(reject|reject all|reject all cookies|reject cookies|decline|decline all|i decline|deny|deny all|refuse|refuse all|opt out|opt-out|opt out of all|necessary only|only necessary|essential only|use necessary cookies only|拒绝|全部拒绝|拒绝全部|仅必要)$/i;
const CLOSE_TEXT = /^(×|✕|✖|x|close|dismiss|no thanks|no, thanks|not now|maybe later|skip|关闭|稍后|以后再说)$/i;
const CLOSE_ATTR = /\b(close|dismiss)\b|关闭/i;
const CONTEXT_ATTR = /(consent|cookie|gdpr|ccpa|cmp|privacy|modal|popup|pop-up|overlay|dialog|banner|notice|newsletter|interstitial|paywall)/i;

function labelOf(el) {
  const text = (el.innerText || el.textContent || el.value || '').replace(/\s+/g, ' ').trim();
  return text.slice(0, 60);
}
function attrsOf(el) {
  return [el.getAttribute('aria-label'), el.getAttribute('title'),
          el.id, typeof el.className === 'string' ? el.className : '']
    .filter(Boolean).join(' ');
}
function isInteractable(el) {
  if (!el || el.hasAttribute(MARK) || el.disabled) return false;
  if (el.getAttribute('aria-disabled') === 'true') return false;
  const rect = el.getBoundingClientRect();
  if (rect.width < 2 || rect.height < 2) return false;
  if (rect.bottom <= 0 || rect.right <= 0 ||
      rect.top >= window.innerHeight || rect.left >= window.innerWidth) return false;
  const style = getComputedStyle(el);
  if (style.visibility === 'hidden' || style.display === 'none' ||
      style.pointerEvents === 'none' || Number(style.opacity) === 0) return false;
  const cx = Math.min(Math.max(rect.left + rect.width / 2, 0), window.innerWidth - 1);
  const cy = Math.min(Math.max(rect.top + rect.height / 2, 0), window.innerHeight - 1);
  const hit = document.elementFromPoint(cx, cy);
  return !!hit && (hit === el || el.contains(hit) || hit.contains(el));
}
function inPopupContext(el) {
  if (inConsentFrame) return true;
  for (let node = el; node && node !== document.body; node = node.parentElement) {
    const role = node.getAttribute && node.getAttribute('role');
    if (role === 'dialog' || role === 'alertdialog') return true;
    if (node.getAttribute && node.getAttribute('aria-modal') === 'true') return true;
    if (node.tagName === 'DIALOG') return true;
    const pos = getComputedStyle(node).position;
    if (pos === 'fixed' || pos === 'sticky') return true;
    const cls = typeof node.className === 'string' ? node.className : '';
    if (CONTEXT_ATTR.test((node.id || '') + ' ' + cls)) return true;
  }
  return false;
}

const candidates = Array.from(document.querySelectorAll(
  'button, [role="button"], a[href="#"], a[role="button"], input[type="button"], input[type="submit"], [aria-label], [class*="close"]'
));
const buckets = {accept: null, reject: null, close: null};
for (const el of candidates) {
  if (buckets.accept && buckets.reject && buckets.close) break;
  const label = labelOf(el);
  const attrs = attrsOf(el);
  let kind = null;
  if (label && ACCEPT.test(label)) kind = 'accept';
  else if (label && REJECT.test(label)) kind = 'reject';
  else if ((label && CLOSE_TEXT.test(label)) || (!label && CLOSE_ATTR.test(attrs)) ||
           (CLOSE_ATTR.test(el.getAttribute('aria-label') || ''))) kind = 'close';
  if (!kind || buckets[kind]) continue;
  if (!isInteractable(el) || !inPopupContext(el)) continue;
  buckets[kind] = {el: el, label: label || attrs.slice(0, 60)};
}
for (const kind of ['accept', 'reject', 'close']) {
  const hit = buckets[kind];
  if (!hit) continue;
  hit.el.setAttribute(MARK, '1');
  try { hit.el.click(); } catch (e) { return {clicked: false, kind: kind, error: String(e)}; }
  return {clicked: true, kind: kind, label: hit.label};
}
return {clicked: false};
"""

# 找出有限数量的可见 consent iframe。
JS_FIND_CONSENT_IFRAMES = r"""
const PATTERN = /(consent|cmp|privacy|cookie|gdpr|sp_message|sourcepoint|onetrust|didomi|quantcast|trustarc|truste|usercentrics|cookiebot)/i;
const out = [];
for (const frame of Array.from(document.querySelectorAll('iframe'))) {
  const key = [frame.src, frame.id, frame.name, frame.title,
               typeof frame.className === 'string' ? frame.className : ''].join(' ');
  if (!PATTERN.test(key)) continue;
  const rect = frame.getBoundingClientRect();
  if (rect.width < 2 || rect.height < 2) continue;
  const style = getComputedStyle(frame);
  if (style.visibility === 'hidden' || style.display === 'none') continue;
  out.push(frame);
  if (out.length >= arguments[0]) break;
}
return out;
"""


# ---------------------------------------------------------------------------
# 采集指标与预算看门狗
# ---------------------------------------------------------------------------

_METRICS = {}
_ACTIVE_WATCHDOG = None
_WATCHDOG_LOCK = threading.Lock()


def get_capture_metrics():
    """返回本进程最近一次访问的采集指标（run_capture.py 合并抓包侧指标后写出）。"""
    return _METRICS


class _BudgetWatchdog:
    """第 106 秒发出预算信号；第 113 秒浏览器仍未结束则强制清理进程。"""

    def __init__(self, started_at, metrics, logger=None):
        self.started_at = started_at
        self.metrics = metrics
        self.logger = logger
        self.budget_event = threading.Event()
        self.done_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self.done_event.set()

    def _wait_until(self, offset_secs):
        remaining = self.started_at + offset_secs - time.monotonic()
        if remaining > 0:
            return self.done_event.wait(remaining)
        return self.done_event.is_set()

    def _run(self):
        if self._wait_until(BUDGET_SIGNAL_SECS):
            return
        self.budget_event.set()
        self.metrics["budget_signal"] = True
        _log_warning(self.logger, f"已到第{BUDGET_SIGNAL_SECS}秒预算边界，浏览器流程开始收尾")
        if self._wait_until(FORCE_CLEANUP_SECS):
            return
        self.metrics["forced_browser_cleanup"] = True
        _log_warning(self.logger, f"已到第{FORCE_CLEANUP_SECS}秒浏览器仍未结束，强制清理浏览器进程")
        chrome_backend.kill_chrome_processes()


def _start_watchdog(started_at, metrics, logger):
    global _ACTIVE_WATCHDOG
    with _WATCHDOG_LOCK:
        if _ACTIVE_WATCHDOG is not None:
            _ACTIVE_WATCHDOG.stop()
        _ACTIVE_WATCHDOG = _BudgetWatchdog(started_at, metrics, logger)
        _ACTIVE_WATCHDOG.start()
        return _ACTIVE_WATCHDOG


def _stop_watchdog():
    global _ACTIVE_WATCHDOG
    with _WATCHDOG_LOCK:
        if _ACTIVE_WATCHDOG is not None:
            _ACTIVE_WATCHDOG.stop()
            _ACTIVE_WATCHDOG = None


# ---------------------------------------------------------------------------
# 驱动创建与进程清理
# ---------------------------------------------------------------------------

def create_chrome_driver(task_name=None, formatted_time=None, parsers=None, **kwargs):
    """复用 chrome.py 的无头 Chrome（含 --ssl-key-log-file），并记录关闭耗时。"""
    created = chrome_backend.create_chrome_driver(
        task_name, formatted_time, parsers, **kwargs
    )
    browser = created[0] if isinstance(created, tuple) else created
    original_quit = browser.quit

    def timed_quit():
        started = time.monotonic()
        try:
            return original_quit()
        finally:
            _METRICS["browser_quit_secs"] = round(time.monotonic() - started, 3)
            _stop_watchdog()

    browser.quit = timed_quit
    return created


def kill_chrome_processes():
    _stop_watchdog()
    chrome_backend.kill_chrome_processes()


# ---------------------------------------------------------------------------
# 访问流程
# ---------------------------------------------------------------------------

class _Visit:
    def __init__(self, driver, url, started_at, watchdog, metrics, logger):
        self.driver = driver
        self.url = url
        self.started_at = started_at
        self.watchdog = watchdog
        self.metrics = metrics
        self.logger = logger

    def elapsed(self):
        return time.monotonic() - self.started_at

    def budget_hit(self):
        return self.watchdog.budget_event.is_set()

    # ---- 弹窗 ----
    def _record_popup_click(self, result, frame_index=None):
        if not isinstance(result, dict) or not result.get("clicked"):
            return False
        clicks = self.metrics.setdefault("popup_clicks", [])
        clicks.append({
            "at": round(self.elapsed(), 2),
            "kind": result.get("kind", ""),
            "label": str(result.get("label", ""))[:60],
            "frame": frame_index,
        })
        if self.logger is not None:
            location = "主页面" if frame_index is None else f"consent iframe#{frame_index}"
            self.logger.info(
                f"弹窗处理[{location}]: {result.get('kind')} -> {result.get('label', '')!r}"
            )
        return True

    def handle_popups(self):
        driver = self.driver
        try:
            if self._record_popup_click(driver.execute_script(JS_HANDLE_POPUPS, False)):
                return
            frames = driver.execute_script(JS_FIND_CONSENT_IFRAMES, MAX_CONSENT_IFRAMES) or []
        except Exception as e:
            self.metrics["popup_errors"] = self.metrics.get("popup_errors", 0) + 1
            if self.metrics["popup_errors"] <= 3:
                _log_warning(self.logger, f"弹窗扫描失败: {type(e).__name__}: {str(e).splitlines()[0]}")
            return

        for index, frame in enumerate(frames[:MAX_CONSENT_IFRAMES]):
            try:
                driver.switch_to.frame(frame)
                result = driver.execute_script(JS_HANDLE_POPUPS, True)
            except Exception:
                result = None
            finally:
                try:
                    driver.switch_to.default_content()
                except Exception:
                    pass
            if self._record_popup_click(result, frame_index=index):
                return

    def pause(self, secs):
        """目标等待 secs 秒，期间反复处理弹窗；第 105 秒或预算信号后不再继续等。"""
        end = time.monotonic() + secs
        hard_end = self.started_at + PAUSE_DEADLINE_SECS
        while True:
            now = time.monotonic()
            if now >= end or now >= hard_end or self.budget_hit():
                break
            scan_started = now
            self.handle_popups()
            spent = time.monotonic() - scan_started
            sleep_for = min(
                POPUP_SCAN_INTERVAL_SECS - spent,
                end - time.monotonic(),
                hard_end - time.monotonic(),
            )
            if sleep_for > 0:
                self.watchdog.budget_event.wait(sleep_for)

    # ---- 页面状态 ----
    def snapshot(self):
        state = self.driver.execute_script(JS_PAGE_SNAPSHOT) or {}
        state["elapsed"] = round(self.elapsed(), 2)
        return state

    @staticmethod
    def at_bottom(state):
        return (
            int(state.get("scrollY") or 0) + int(state.get("innerHeight") or 0)
            >= int(state.get("totalHeight") or 0) - BOTTOM_TOLERANCE_PX
        )

    # ---- 导航 ----
    def navigate(self):
        driver = self.driver
        page_load_timeout = max(
            1.0,
            min(PAGE_LOAD_TIMEOUT_SECS, NAVIGATION_DEADLINE_SECS - self.elapsed()),
        )
        try:
            driver.set_script_timeout(SCRIPT_TIMEOUT_SECS)
            driver.set_page_load_timeout(page_load_timeout)
        except Exception as e:
            _log_warning(self.logger, f"设置超时失败: {type(e).__name__}: {e}")

        nav_started = time.monotonic()
        self.metrics["navigation_timeout"] = False
        try:
            driver.get(self.url)
        except TimeoutException:
            # 不 window.stop()：允许页面脚本、挑战脚本与重定向继续执行。
            self.metrics["navigation_timeout"] = True
            _log_warning(
                self.logger,
                f"navigation_timeout: 导航等待超过{page_load_timeout:.1f}秒，继续后续流程",
            )
        self.metrics["navigation_secs"] = round(time.monotonic() - nav_started, 3)

    def maybe_import_cookies(self):
        host = (urlparse(self.url).hostname or "").lower()
        for domain, cookie_file in COOKIE_FILES.items():
            normalized = str(domain).strip(".").lower()
            if host == normalized or host.endswith(f".{normalized}"):
                if not os.path.exists(cookie_file):
                    _log_warning(self.logger, f"Cookie 文件不存在: {cookie_file}")
                    return
                add_cookies(self.driver, cookie_file)
                self.driver.refresh()
                self.metrics["cookies_imported"] = cookie_file
                return

    # ---- 主流程 ----
    def run(self):
        self.metrics["status"] = "navigating"
        self.navigate()

        self.metrics["status"] = "initial_wait"
        self.pause(INITIAL_WAIT_SECS)
        self.maybe_import_cookies()
        self.metrics["initial_snapshot"] = self.snapshot()

        self.metrics["status"] = "scrolling"
        scroll_count = 0
        reached_bottom = False
        while True:
            state = self.snapshot()
            if self.at_bottom(state):
                reached_bottom = True
                break
            if self.elapsed() >= SCROLL_LOOP_DEADLINE_SECS or self.budget_hit():
                break
            delta = max(1, int(int(state.get("totalHeight") or 0) * SCROLL_FRACTION))
            self.driver.execute_script(JS_SMOOTH_SCROLL_BY, delta)
            scroll_count += 1
            self.metrics["scroll_count"] = scroll_count
            self.pause(SCROLL_WAIT_SECS)

        self.metrics["scroll_count"] = scroll_count
        self.metrics["scroll_bottom_reached"] = reached_bottom
        if not reached_bottom:
            self.metrics["budget_reached"] = True
            self.metrics["status"] = "budget_reached"
            _log_warning(
                self.logger,
                f"滚动预算结束仍未到底: scroll_bottom_reached=False, budget_reached, "
                f"scroll_count={scroll_count}",
            )
            return

        self.metrics["status"] = "bottom_wait"
        self.pause(BOTTOM_WAIT_SECS)
        if self.budget_hit():
            self.metrics["budget_reached"] = True
            self.metrics["status"] = "budget_reached"
        else:
            self.metrics["status"] = "completed"


def _screenshot_timeout(started_at):
    remaining = started_at + FORCE_CLEANUP_SECS - 2 - time.monotonic()
    return max(MIN_SCREENSHOT_TIMEOUT_SECS, remaining)


def _finalize(visit, ssl_key_file_path, save_screenshot):
    """尽力保存最终快照、当前窗口截图、HTML 与文本；单步失败不影响其他步骤。"""
    driver = visit.driver
    metrics = visit.metrics
    logger = visit.logger
    errors = metrics.setdefault("finalize_errors", [])

    def step(name, func):
        try:
            return func()
        except Exception as e:
            errors.append(f"{name}: {type(e).__name__}: {str(e).splitlines()[0][:200]}")
            _log_warning(logger, f"收尾步骤 {name} 失败: {type(e).__name__}: {e}")
            return None

    content_path = ssl_key_file_path.replace("_ssl_key.log", ".txt").replace("/ssl_key/", "/content/")
    html_path = ssl_key_file_path.replace("_ssl_key.log", ".html").replace("/ssl_key/", "/html/")
    screenshot_path = None

    final_snapshot = step("final_snapshot", visit.snapshot)
    if final_snapshot is not None:
        metrics["final_snapshot"] = final_snapshot

    if save_screenshot:
        screenshot_path = ssl_key_file_path.replace("_ssl_key.log", ".png").replace("/ssl_key/", "/screenshot/")
        os.makedirs(os.path.dirname(screenshot_path), exist_ok=True)

        def save_viewport():
            timeout = _screenshot_timeout(visit.started_at)
            try:
                saved = chrome_backend._call_with_worker_timeout(
                    timeout,
                    f"当前窗口截图超过{timeout:.0f}秒",
                    lambda: driver.save_screenshot(screenshot_path),
                )
            except TimeoutError:
                chrome_backend._mark_driver_skip_quit(driver)
                raise
            if not saved:
                raise RuntimeError("driver.save_screenshot returned False")

        step("screenshot", save_viewport)

    def save_html():
        html = driver.page_source
        os.makedirs(os.path.dirname(html_path), exist_ok=True)
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html)

    step("html", save_html)

    def save_content():
        script = chrome_backend.JS_SELECT_ALL_AND_COPY_CAPTURE + "\nreturn __select_all_and_copy_capture();"
        res = driver.execute_script(script)
        if not isinstance(res, dict) or res.get("error"):
            raise RuntimeError(f"JS失败: {res}")
        plain = re.sub(
            r'(?:[ \t\f 　​‌‍﻿⁠­\v]*\r?\n)+',
            '\n',
            res.get("plain", ""),
        )
        os.makedirs(os.path.dirname(content_path), exist_ok=True)
        with open(content_path, "w", encoding="utf-8") as f:
            f.write(plain)

    step("content", save_content)

    current_url = step("current_url", lambda: driver.current_url) or visit.url
    metrics["browser_visit_secs"] = round(visit.elapsed() - metrics.get("visit_started_at", 0), 3)
    if not errors:
        metrics.pop("finalize_errors", None)
    return content_path, html_path, screenshot_path, current_url


def open_url_and_save_content(driver, url, ssl_key_file_path, wait_secs=None,
                              save_screenshot=True, data_base_dir=None, logger=None,
                              capture_started_at=None):
    """按逐段下滑流程访问 URL 并保存内容，返回值与 chrome.py 保持一致。"""
    del wait_secs, data_base_dir  # 时间由本模块预算常量控制
    started_at = capture_started_at if capture_started_at is not None else time.monotonic()

    _METRICS.clear()
    _METRICS.update({
        "flow": "chrome_scroll",
        "status": "starting",
        "visit_started_at": round(time.monotonic() - started_at, 3),
        "scroll_count": 0,
        "scroll_bottom_reached": False,
        "budget_reached": False,
        "forced_browser_cleanup": False,
    })
    watchdog = _start_watchdog(started_at, _METRICS, logger)
    visit = _Visit(driver, url, started_at, watchdog, _METRICS, logger)

    run_error = None
    try:
        visit.run()
    except Exception as e:
        run_error = e
        _METRICS["status"] = "error"
        _METRICS["error"] = f"{type(e).__name__}: {str(e).splitlines()[0][:300] if str(e) else ''}"
        _log_warning(logger, f"访问流程异常，进入收尾: {_METRICS['error']}")

    result = _finalize(visit, ssl_key_file_path, save_screenshot)
    if logger is not None:
        summary = {k: v for k, v in _METRICS.items()
                   if k not in ("initial_snapshot", "final_snapshot", "popup_clicks")}
        logger.info("采集指标: " + json.dumps(summary, ensure_ascii=False, default=str))
    if run_error is not None:
        raise run_error
    return result
