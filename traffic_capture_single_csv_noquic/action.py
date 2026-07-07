"""
traffic_capture_x/action.py - X(Twitter)流量捕获
继承BaseAction
"""
import sys
import os

# 添加当前目录到路径（容器内 /app 包含 tools/ 子目录）
_current_dir: str = os.path.dirname(os.path.abspath(__file__))
if _current_dir not in sys.path:
    sys.path.insert(0, _current_dir)

from tools.base_action import BaseAction
from tools.chrome_quic import (
    create_chrome_driver,
    kill_chrome_processes,
    open_url_and_save_content,
)


class XCaptureAction(BaseAction):
    """流量捕获Action"""

    # 使用默认阈值
    pcap_lowest_size = 100000
    ssl_key_lowest_size = 128

    def kill_browser_processes(self):
        kill_chrome_processes()

    def create_browser_driver(self, formatted_time, row_id, artifact_label=None):
        return create_chrome_driver(
            self.allowed_domain,
            formatted_time,
            f"{row_id}",
            data_base_dir=_current_dir,
            proxy_server=self.get_browser_proxy_server(),
            proxy_bypass_list=self.get_browser_proxy_bypass_list(),
            logger=self.logger,
            artifact_label=artifact_label,
        )

    def open_and_save_content(self, browser, url, ssl_key_file_path):
        return open_url_and_save_content(
            browser,
            url,
            ssl_key_file_path,
            data_base_dir=_current_dir,
            logger=self.logger,
        )


if __name__ == "__main__":
    XCaptureAction.run_from_argv()
