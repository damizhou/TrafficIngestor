"""
traffic_capture_single_csv_firefox/action.py - Firefox 流量捕获
继承BaseAction
"""
import sys
import os

# 添加当前目录到路径（容器内 /app 包含 tools/ 子目录）
_current_dir = os.path.dirname(os.path.abspath(__file__))
if _current_dir not in sys.path:
    sys.path.insert(0, _current_dir)

from tools.base_action import BaseAction
from tools.firefox import (
    create_firefox_driver,
    kill_firefox_processes,
    open_url_and_save_content,
)

FIREFOX_BACKGROUND_CAPTURE_EXCLUDE_HOSTS = (
    "firefox.settings.services.mozilla.com",
    "content-signature-2.cdn.mozilla.net",
    "firefox-settings-attachments.cdn.mozilla.net",
)


def _host_matches_domain(host, domain):
    normalized_host = (host or "").strip(".").lower()
    normalized_domain = (domain or "").strip(".").lower()
    if not normalized_host or not normalized_domain:
        return False
    return (
        normalized_host == normalized_domain
        or normalized_host.endswith(f".{normalized_domain}")
        or normalized_domain.endswith(f".{normalized_host}")
    )


class XCaptureFirefoxAction(BaseAction):
    """Firefox 流量捕获 Action"""

    pcap_lowest_size = 100000
    ssl_key_lowest_size = 1000
    browser_name = "Firefox"
    def kill_browser_processes(self):
        kill_firefox_processes()

    def create_browser_driver(self, formatted_time, row_id):
        return create_firefox_driver(
            self.allowed_domain, formatted_time, f"{row_id}",
            data_base_dir=_current_dir
        )

    def open_and_save_content(self, browser, url, ssl_key_file_path):
        return open_url_and_save_content(
            browser, url, ssl_key_file_path,
            data_base_dir=_current_dir
        )

    def get_capture_exclude_hosts(self):
        return tuple(
            host
            for host in FIREFOX_BACKGROUND_CAPTURE_EXCLUDE_HOSTS
            if not _host_matches_domain(host, self.allowed_domain)
        )


if __name__ == "__main__":
    XCaptureFirefoxAction.run_from_argv()
