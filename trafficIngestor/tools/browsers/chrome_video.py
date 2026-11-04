"""Chrome 视频采集后端：在 chrome 基类之上扩展 consent 处理与视频起播。

与 chrome.py 的差别只有两处：
1. 采集窗口放宽到 4 分钟：播放器大约在页面加载完 30 秒后才创建，
   首个 HLS 分片最晚在 2 分钟出现，窗口太短会一片都抓不到；
2. 页面加载完成后先点掉 consent 弹层，再拉起视频播放，之后才进入采集窗口。
"""

import time

from selenium.common.exceptions import WebDriverException
from selenium.webdriver.common.by import By

try:
    from tools.browsers import chrome as chrome_backend
except ImportError:  # 直接以脚本方式加载
    import chrome as chrome_backend

POST_LOAD_CAPTURE_WAIT_SECS = 240

_log_warning = chrome_backend._log_warning

# 视频自动播放：页面打开后用静音 JS 拉起视频，否则采集期只有封面图，
# pcap 中不会出现媒体流。启动参数已放开自动播放策略，无需滚动。
ENABLE_VIDEO_PLAYBACK = True
VIDEO_PLAYBACK_CONFIRM_SECS = 3.0
VIDEO_PLAYBACK_MIN_PROGRESS_SECS = 0.5
VIDEO_PLAYBACK_POLL_INTERVAL_SECS = 0.5
VIDEO_PLAYBACK_ELEMENT_WAIT_SECS = 45.0
# 播放器拿到媒体源通常比页面加载晚十几秒，起播尝试要在该窗口内持续重试。
VIDEO_PLAYBACK_RETRY_SECS = 60.0
VIDEO_PLAYBACK_RETRY_INTERVAL_SECS = 2.0
VIDEO_PLAYBACK_CLICK_RETRY_SECS = 10.0

# consent 墙（CMP）：未同意时站点会禁用视频播放器（如 Primis 直接不下发 <video>），
# 因此需要先点掉同意按钮，播放器才会被创建。
ENABLE_CONSENT_ACCEPT = True
CONSENT_SETTLE_SECS = 2.0

# 点击兜底：视频广告播放器在 JS play() 之后仍可能保持暂停，需要一次真实点击才起播。
ENABLE_VIDEO_CLICK_FALLBACK = True
VIDEO_PLAYBACK_CLICK_SETTLE_SECS = 1.5
VIDEO_PLAYBACK_CLICK_CONFIRM_SECS = 10.0


# 读取页面内所有 video 元素状态（含 open shadow DOM）。
JS_COLLECT_VIDEO_STATES = r"""
function __collectVideoStates(){
  const videos = [];
  const walk = (root) => {
    root.querySelectorAll('video').forEach((v) => videos.push(v));
    root.querySelectorAll('*').forEach((el) => { if (el.shadowRoot) walk(el.shadowRoot); });
  };
  walk(document);
  return videos.map((v) => {
    const rect = v.getBoundingClientRect();
    return {
      paused: v.paused,
      currentTime: v.currentTime,
      readyState: v.readyState,
      networkState: v.networkState,
      frames: v.webkitDecodedFrameCount === undefined ? -1 : v.webkitDecodedFrameCount,
      width: Math.round(rect.width),
      height: Math.round(rect.height),
      hasSource: !!(v.currentSrc || v.getAttribute('src') || v.srcObject),
    };
  });
}
return __collectVideoStates();
"""

# 兜底的 JS 起播：静音播放，避免被自动播放策略拦下。
JS_START_VIDEO_PLAYBACK = r"""
function __startVideoPlayback(){
  let started = 0;
  const walk = (root) => {
    root.querySelectorAll('video').forEach((v) => {
      try {
        if (!v.paused) return;
        v.muted = true;
        v.volume = 0;
        const result = v.play();
        if (result && typeof result.catch === 'function') result.catch(() => {});
        started += 1;
      } catch (e) {}
    });
    root.querySelectorAll('*').forEach((el) => { if (el.shadowRoot) walk(el.shadowRoot); });
  };
  walk(document);
  return started;
}
return __startVideoPlayback();
"""

# 点击 consent 弹层里的同意按钮（含 open shadow DOM 与同源 iframe）。
JS_ACCEPT_CONSENT = r"""
function __consentRoots(){
  const roots = [document];
  const walk = (root, depth) => {
    if (depth > 3 || !root.querySelectorAll) return;
    root.querySelectorAll('*').forEach((el) => {
      if (el.shadowRoot) { roots.push(el.shadowRoot); walk(el.shadowRoot, depth + 1); }
    });
    root.querySelectorAll('iframe').forEach((frame) => {
      try {
        if (frame.contentDocument) { roots.push(frame.contentDocument); walk(frame.contentDocument, depth + 1); }
      } catch (e) {}
    });
  };
  walk(document, 0);
  return roots;
}
function __consentLabel(el){
  return ((el.innerText || el.textContent || '') + ' ' + (el.getAttribute('aria-label') || ''))
    .replace(/\s+/g, ' ').trim().toLowerCase();
}
function __consentVisible(el){
  try {
    const style = window.getComputedStyle(el);
    if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') return false;
    const rect = el.getBoundingClientRect();
    return rect.width >= 4 && rect.height >= 4;
  } catch (e) { return false; }
}
function __acceptConsent(){
  const exact = ['accept all', 'accept', 'i accept', 'agree', 'i agree', 'allow all', 'got it',
                 '\u540c\u610f', '\u63a5\u53d7\u5168\u90e8', '\u63a5\u53d7'];
  const selector = 'button, a, [role="button"], input[type="button"], input[type="submit"]';
  const roots = __consentRoots();
  for (const label of exact) {
    for (const root of roots) {
      for (const el of Array.from(root.querySelectorAll(selector))) {
        if (el.disabled || !__consentVisible(el)) continue;
        if (__consentLabel(el) === label) { el.click(); return label; }
      }
    }
  }
  for (const root of roots) {
    for (const el of Array.from(root.querySelectorAll(selector))) {
      const label = __consentLabel(el);
      if (!label || label.length > 40) continue;
      if (!/(accept|agree)/.test(label)) continue;
      if (/(reject|deny|decline|manage|settings|options|more)/.test(label)) continue;
      if (el.disabled || !__consentVisible(el)) continue;
      el.click();
      return label;
    }
  }
  return '';
}
return __acceptConsent();
"""

# 隐藏 CMP 留下的全屏遮罩层：qc-cmp 点完同意后仍会留一层 fixed 全屏 div，
# 挡住播放器导致真实点击被拦截。
JS_DISMISS_BLOCKING_OVERLAYS = r"""
function __dismissBlockingOverlays(){
  let hidden = 0;
  const minWidth = window.innerWidth * 0.5;
  const minHeight = window.innerHeight * 0.5;
  document.querySelectorAll('div, section, aside').forEach((el) => {
    const cls = (el.className && el.className.toString) ? el.className.toString() : '';
    const id = el.id || '';
    if (!/(qc-cmp|cmp-|consent|privacy|gdpr)/i.test(cls + ' ' + id)) return;
    const rect = el.getBoundingClientRect();
    if (rect.width < minWidth || rect.height < minHeight) return;
    const style = window.getComputedStyle(el);
    if (style.position !== 'fixed' && style.position !== 'absolute') return;
    el.style.setProperty('display', 'none', 'important');
    el.style.setProperty('pointer-events', 'none', 'important');
    hidden += 1;
  });
  return hidden;
}
return __dismissBlockingOverlays();
"""


def _video_state_is_playing(state):
    if not isinstance(state, dict):
        return False
    # paused=False 表示 play() 已被接受（可能仍在缓冲），视为已起播。
    if state.get("paused") is False:
        return True
    if state.get("currentTime", 0) > VIDEO_PLAYBACK_MIN_PROGRESS_SECS:
        return True
    return state.get("frames", 0) > 0


def _collect_page_video_states(driver):
    return driver.execute_script(JS_COLLECT_VIDEO_STATES) or []


def _accept_page_consent(driver, logger=None):
    """点掉 consent 弹层里的同意按钮，返回被点击按钮的文本；没找到返回空串。

    站点通常在未同意时不创建视频播放器（如 Primis 直接不下发 <video>），
    所以这一步要排在触发播放之前。
    """
    if not ENABLE_CONSENT_ACCEPT:
        return ""
    try:
        label = driver.execute_script(JS_ACCEPT_CONSENT)
    except Exception as e:
        _log_warning(logger, f"处理 consent 弹层失败: {type(e).__name__}: {e}")
        return ""
    if not label:
        return ""
    if logger:
        logger.info(f"cookie 同意: 已点击「{label}」")
    # CMP 点完同意后常留一层全屏遮罩，顺手清掉，否则后续点击播放器会被拦截。
    _dismiss_blocking_overlays(driver, logger)
    time.sleep(CONSENT_SETTLE_SECS)
    return label


def _close_extra_browser_windows(driver, original_handles, logger=None):
    """点击播放器可能触发新窗口，及时关掉以免产物和抓包被带偏。"""
    try:
        handles = driver.window_handles
    except Exception as e:
        _log_warning(logger, f"读取窗口列表失败: {type(e).__name__}: {e}")
        return
    for handle in handles:
        if handle in original_handles:
            continue
        try:
            driver.switch_to.window(handle)
            driver.close()
        except Exception as e:
            _log_warning(logger, f"关闭视频点击弹出的窗口失败: {type(e).__name__}: {e}")
    try:
        if driver.window_handles and driver.current_window_handle not in original_handles:
            driver.switch_to.window(original_handles[0])
    except Exception as e:
        _log_warning(logger, f"切回原始窗口失败: {type(e).__name__}: {e}")


def _wait_for_video_states(driver, deadline, logger=None):
    """等待页面出现 video 元素（consent 通过后播放器是异步创建的）。"""
    states = []
    while time.time() < deadline:
        try:
            states = _collect_page_video_states(driver)
        except Exception as e:
            _log_warning(logger, f"读取页面视频状态失败: {type(e).__name__}: {e}")
            break
        if states:
            return states
        time.sleep(VIDEO_PLAYBACK_POLL_INTERVAL_SECS)
    return states


def _dismiss_blocking_overlays(driver, logger=None):
    """隐藏 CMP 遗留的全屏遮罩，避免播放器点击被拦截。"""
    try:
        hidden = driver.execute_script(JS_DISMISS_BLOCKING_OVERLAYS)
    except Exception as e:
        _log_warning(logger, f"隐藏 consent 遮罩失败: {type(e).__name__}: {e}")
        return 0
    if hidden and logger:
        logger.info(f"cookie 同意: 已隐藏 {hidden} 个遗留遮罩层")
    return hidden


def _click_first_visible_video(driver, logger=None):
    """点击兜底：视频广告播放器在 JS play() 后仍可能保持暂停，需要一次真实点击。"""
    original_handles = []
    try:
        original_handles = driver.window_handles
    except Exception:
        original_handles = []

    _dismiss_blocking_overlays(driver, logger)

    try:
        elements = driver.find_elements(By.CSS_SELECTOR, "video")
    except Exception as e:
        _log_warning(logger, f"定位 video 元素失败: {type(e).__name__}: {e}")
        return False

    for element in elements:
        try:
            if not element.is_displayed():
                continue
            driver.execute_script("arguments[0].scrollIntoView({block: 'nearest'});", element)
            time.sleep(0.3)
            try:
                element.click()
            except WebDriverException as e:
                # 遮罩仍在前台时退回 JS 点击，绕过命中测试。
                _log_warning(logger, f"真实点击被拦截，改用 JS 点击: {type(e).__name__}")
                driver.execute_script("arguments[0].click();", element)
        except Exception as e:
            _log_warning(logger, f"点击 video 元素失败: {type(e).__name__}: {e}")
            continue

        time.sleep(VIDEO_PLAYBACK_CLICK_SETTLE_SECS)
        if original_handles:
            _close_extra_browser_windows(driver, original_handles, logger=logger)
        return True

    return False


def _wait_for_video_playback(driver, deadline, logger=None):
    """轮询等待视频时间轴推进，返回 (是否起播, 最后一次读到的状态列表)。"""
    last_states = []
    while time.time() < deadline:
        try:
            last_states = _collect_page_video_states(driver)
        except Exception as e:
            _log_warning(logger, f"确认视频播放状态失败: {type(e).__name__}: {e}")
            break
        if any(_video_state_is_playing(state) for state in last_states):
            return True, last_states
        time.sleep(VIDEO_PLAYBACK_POLL_INTERVAL_SECS)
    return False, last_states


def _log_video_playback_started(logger, states, source):
    if not logger:
        return
    first = states[0] if states else {}
    logger.info(
        "视频检测: {}起播，currentTime={}，已解码帧={}".format(
            source,
            round(float(first.get("currentTime") or 0), 2),
            first.get("frames"),
        )
    )


def _trigger_video_playback(driver, logger=None):
    """拉起页面视频，确保采集窗口内存在真实媒体流量。

    顺序：等 video 元素出现 → 静音 JS play()（启动参数已放开自动播放策略）→
    仍不起播时退化为一次真实点击（视频广告播放器常要求交互）。
    没有 video 元素的页面直接跳过，不产生额外等待。
    返回 True 表示确认到视频已进入播放状态。
    """
    if not ENABLE_VIDEO_PLAYBACK:
        return False

    # 先确认页面存在播放器；没有 video 元素的页面立即跳过，不引入等待。
    states = _wait_for_video_states(
        driver, time.time() + VIDEO_PLAYBACK_ELEMENT_WAIT_SECS, logger
    )
    if not states:
        if logger:
            logger.info(
                f"视频检测: 等待{int(VIDEO_PLAYBACK_ELEMENT_WAIT_SECS)}秒仍未出现 video 元素，跳过起播"
            )
        return False

    # 播放器拿到媒体源通常比页面加载晚十几秒，期间持续重试 JS 起播与点击兜底。
    deadline = time.time() + VIDEO_PLAYBACK_RETRY_SECS
    last_click_at = 0.0
    while True:
        if any(_video_state_is_playing(state) for state in states):
            _log_video_playback_started(logger, states, "自动播放")
            return True

        if any(state.get("hasSource") for state in states):
            try:
                driver.execute_script(JS_START_VIDEO_PLAYBACK)
            except Exception as e:
                _log_warning(logger, f"JS 触发视频播放失败: {type(e).__name__}: {e}")

            playing, states = _wait_for_video_playback(
                driver, time.time() + VIDEO_PLAYBACK_CONFIRM_SECS, logger
            )
            if playing:
                _log_video_playback_started(logger, states, "JS 自动播放")
                return True

        if (
            ENABLE_VIDEO_CLICK_FALLBACK
            and time.time() - last_click_at >= VIDEO_PLAYBACK_CLICK_RETRY_SECS
            and _click_first_visible_video(driver, logger=logger)
        ):
            last_click_at = time.time()
            playing, states = _wait_for_video_playback(
                driver, time.time() + VIDEO_PLAYBACK_CLICK_CONFIRM_SECS, logger
            )
            if playing:
                _log_video_playback_started(logger, states, "点击")
                return True

        if time.time() >= deadline:
            break

        time.sleep(VIDEO_PLAYBACK_RETRY_INTERVAL_SECS)
        try:
            states = _collect_page_video_states(driver)
        except Exception as e:
            _log_warning(logger, f"读取页面视频状态失败: {type(e).__name__}: {e}")
            return False
        if not states:
            return False

    if logger:
        first = states[0] if states else {}
        logger.info(
            "视频检测: 未起播，paused={}，readyState={}，networkState={}，有媒体源={}".format(
                first.get("paused"), first.get("readyState"),
                first.get("networkState"), first.get("hasSource"),
            )
        )
    return False


class ChromeVideoAction(chrome_backend.ChromeAction):
    """在基类采集流程上扩展 consent 处理与视频起播。"""

    POST_LOAD_CAPTURE_WAIT_SECS = POST_LOAD_CAPTURE_WAIT_SECS

    def after_page_load(self, driver, logger=None):
        _accept_page_consent(driver, logger)
        _trigger_video_playback(driver, logger)
        return None


_VIDEO_ACTION = ChromeVideoAction()

create_chrome_driver = chrome_backend.create_chrome_driver
get_main_document_response = chrome_backend.get_main_document_response


def open_url_and_save_content(driver, url, ssl_key_file_path, wait_secs=8,
                              save_screenshot=True, data_base_dir=None, logger=None):
    """模块级入口：走视频版采集流程。"""
    return _VIDEO_ACTION.open_url_and_save_content(
        driver, url, ssl_key_file_path, wait_secs=wait_secs,
        save_screenshot=save_screenshot, data_base_dir=data_base_dir, logger=logger,
    )


def __getattr__(name):
    """未在本模块定义的名字回落到 chrome 基类模块，保持 action.py 接口一致。"""
    return getattr(chrome_backend, name)
