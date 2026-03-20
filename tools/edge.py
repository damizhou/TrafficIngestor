"""
统一的Edge浏览器驱动模块
基于Chromium Edge，复用Chrome的内容提取、截图与Cookie处理逻辑。
"""
import os
import sys
import shutil
import subprocess
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.edge.service import Service

# 添加项目根目录到路径
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from tools.chrome import (
    add_cookies,
    open_url_and_save_content,
    screenshot_full_page,
)


EDGE_BINARY_PATH = "/usr/bin/microsoft-edge"


def _normalize_hosts(hosts):
    """规范化主机名，避免空值、重复值和大小写差异。"""
    normalized_hosts = []
    seen = set()

    for host in hosts or ():
        normalized_host = str(host or "").strip().strip(".").lower()
        if not normalized_host or normalized_host in seen:
            continue
        seen.add(normalized_host)
        normalized_hosts.append(normalized_host)

    return tuple(normalized_hosts)


def _build_host_resolver_rules(blocked_hosts):
    """构造 Chromium host-resolver-rules，让目标主机在解析阶段直接失败。"""
    return ",".join(
        f"MAP {host} ^NOTFOUND"
        for host in _normalize_hosts(blocked_hosts)
    )


def _build_blocked_url_patterns(blocked_hosts):
    """构造 DevTools URL 拦截规则，作为 host-resolver-rules 的补充。"""
    return [
        f"*://{host}/*"
        for host in _normalize_hosts(blocked_hosts)
    ]


def _resolve_from_candidates(env_keys, executable_names, common_paths):
    """按环境变量、PATH、常见路径顺序查找可执行文件。"""
    for env_key in env_keys:
        candidate = os.environ.get(env_key)
        if candidate and os.path.exists(candidate):
            return candidate

    for executable_name in executable_names:
        candidate = shutil.which(executable_name)
        if candidate:
            return candidate

    for candidate in common_paths:
        if os.path.exists(candidate):
            return candidate

    return None


def _resolve_edge_driver():
    """查找 EdgeDriver 路径。"""
    return _resolve_from_candidates(
        env_keys=("MSEDGEDRIVER", "EDGE_DRIVER", "EDGEWEBDRIVER"),
        executable_names=("msedgedriver",),
        common_paths=(
            "/usr/local/bin/msedgedriver",
            "/usr/bin/msedgedriver",
        ),
    )


def create_edge_driver(task_name=None, formatted_time=None, parsers=None,
                       enable_ssl_key_log=True, data_base_dir=None, blocked_hosts=None):
    """
    创建Edge浏览器驱动

    Args:
        task_name: 任务名称，用于生成文件名
        formatted_time: 格式化的时间字符串
        parsers: 解析器名称/前缀
        enable_ssl_key_log: 是否启用SSL密钥日志（默认True）
        data_base_dir: 数据基础目录，默认为项目根目录下的相对路径
        blocked_hosts: 浏览器启动期就需要阻断的主机列表

    Returns:
        browser: WebDriver实例
        ssl_key_file_path: SSL密钥日志文件路径（如果启用）
    """
    if data_base_dir is None:
        data_base_dir = _project_root

    ssl_key_file_path = None
    if enable_ssl_key_log and task_name and formatted_time:
        current_time = datetime.now()
        current_data = current_time.strftime("%Y%m%d")
        ssl_key_dir = os.path.join(data_base_dir, "ssl_key", current_data)
        os.makedirs(ssl_key_dir, exist_ok=True)

        filename_prefix = f'{parsers}_' if parsers else ''
        ssl_key_file_path = os.path.join(
            ssl_key_dir,
            f"{filename_prefix}{formatted_time}_{task_name}_ssl_key.log"
        )

    download_folder = os.path.join(os.getcwd(), 'download')
    os.makedirs(download_folder, exist_ok=True)

    os.environ["SE_OFFLINE"] = "true"
    normalized_blocked_hosts = _normalize_hosts(blocked_hosts)
    _ACCEPT_LANGUAGE = "zh-CN,zh;q=0.9"
    _LANG_PRIMARY = "zh-CN"
    _DISABLED_EDGE_FEATURES = ",".join((
        "AsyncDns",
        "AutofillServerCommunication",
        "CertificateTransparencyComponentUpdater",
        "DialMediaRouteProvider",
        "InterestFeedContentSuggestions",
        "MediaRouter",
        "OptimizationHints",
        "Translate",
    ))

    edge_options = Options()

    if not os.path.exists(EDGE_BINARY_PATH):
        raise FileNotFoundError(f"未找到 Microsoft Edge 浏览器二进制：{EDGE_BINARY_PATH}")
    edge_options.binary_location = EDGE_BINARY_PATH

    edge_options.add_argument('--headless')
    edge_options.add_argument("--disable-gpu")
    edge_options.add_argument(f"--disable-features={_DISABLED_EDGE_FEATURES}")
    edge_options.add_argument("--disable-async-dns")
    edge_options.add_argument("--no-sandbox")
    edge_options.add_argument("--disable-dev-shm-usage")
    edge_options.add_argument("--inprivate")
    edge_options.add_argument("--disable-application-cache")
    edge_options.add_argument("--disable-breakpad")
    edge_options.add_argument("--disable-client-side-phishing-detection")
    edge_options.add_argument("--disable-component-extensions-with-background-pages")
    edge_options.add_argument("--disable-component-update")
    edge_options.add_argument("--disable-default-apps")
    edge_options.add_argument("--disable-domain-reliability")
    edge_options.add_argument("--disable-extensions")
    edge_options.add_argument("--disable-infobars")
    edge_options.add_argument("--disable-software-rasterizer")
    edge_options.add_argument("--autoplay-policy=no-user-gesture-required")
    edge_options.add_argument(f"--lang={_LANG_PRIMARY}")
    edge_options.add_argument("--disable-background-networking")
    edge_options.add_argument("--disable-sync")
    edge_options.add_argument("--dns-prefetch-disable")
    edge_options.add_argument("--metrics-recording-only")
    edge_options.add_argument("--no-first-run")
    edge_options.add_argument("--no-default-browser-check")
    edge_options.add_argument("--no-pings")
    edge_options.add_argument("--homepage=about:blank")
    edge_options.add_argument("--log-net-log=/tmp/netlog.json")
    edge_options.add_argument("--net-log-capture-mode=Everything")
    if normalized_blocked_hosts:
        edge_options.add_argument(
            f"--host-resolver-rules={_build_host_resolver_rules(normalized_blocked_hosts)}"
        )

    if ssl_key_file_path:
        edge_options.add_argument(f"--ssl-key-log-file={ssl_key_file_path}")
        print(f"SSL 密钥日志文件路径: {ssl_key_file_path}")

    prefs = {
        "alternate_error_pages.enabled": False,
        "autofill.credit_card_enabled": False,
        "autofill.profile_enabled": False,
        "profile.default_content_settings.popups": 0,
        "credentials_enable_service": False,
        "dns_prefetching.enabled": False,
        "profile.password_manager_enabled": False,
        "profile.password_manager_leak_detection": False,
        "download.default_directory": download_folder,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "net.network_prediction_options": 2,
        "safebrowsing.disable_download_protection": True,
        "safebrowsing.enabled": False,
        "signin.allowed_on_next_startup": False,
        "translate.enabled": False,
        "intl.accept_languages": _ACCEPT_LANGUAGE,
    }
    edge_options.add_experimental_option("prefs", prefs)
    edge_options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    edge_driver = _resolve_edge_driver()
    if edge_driver:
        service = Service(executable_path=edge_driver)
    else:
        service = Service()

    browser = webdriver.Edge(service=service, options=edge_options)
    browser.execute_cdp_cmd('Network.enable', {})
    if normalized_blocked_hosts:
        try:
            browser.execute_cdp_cmd(
                'Network.setBlockedURLs',
                {'urls': _build_blocked_url_patterns(normalized_blocked_hosts)}
            )
        except Exception as e:
            print(f"设置 Edge URL 拦截规则失败，将仅依赖 host-resolver-rules: {e}")
    browser.execute_cdp_cmd('Network.setExtraHTTPHeaders', {'headers': {'Accept-Language': _ACCEPT_LANGUAGE}})
    browser.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument',
                            {'source': '''
                            Object.defineProperty(navigator,"webdriver",{get:()=>undefined});
                            Object.defineProperty(navigator,"language",{get:()=> "zh-CN"});
                            Object.defineProperty(navigator,"languages",{get:()=> ["zh-CN","zh"]});
                            '''.strip()})

    if ssl_key_file_path:
        return browser, ssl_key_file_path
    return browser


def kill_edge_processes():
    """清除 Edge 浏览器进程。"""
    try:
        for pattern in ("msedgedriver", "microsoft-edge", "microsoft-edge-stable", "msedge"):
            subprocess.run(
                ["pkill", "-KILL", "-f", pattern],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
    except Exception as e:
        print(f"Error occurred: {e}")


__all__ = [
    "create_edge_driver",
    "kill_edge_processes",
    "open_url_and_save_content",
    "screenshot_full_page",
    "add_cookies",
]
