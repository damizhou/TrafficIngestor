"""
traffic_capture_single_csv_edge/action.py - Edge 流量捕获
继承BaseAction
"""
import sys
import os

# 添加当前目录到路径（容器内 /app 包含 tools/ 子目录）
_current_dir = os.path.dirname(os.path.abspath(__file__))
if _current_dir not in sys.path:
    sys.path.insert(0, _current_dir)

from tools.base_action import BaseAction
from tools.edge import create_edge_driver, open_url_and_save_content, kill_edge_processes


class XCaptureEdgeAction(BaseAction):
    """Edge 流量捕获 Action"""

    pcap_lowest_size = 100000
    ssl_key_lowest_size = 1000
    browser_name = "Edge"

    def kill_browser_processes(self):
        kill_edge_processes()

    def create_browser_driver(self, formatted_time, row_id):
        return create_edge_driver(
            self.allowed_domain, formatted_time, f"{row_id}",
            data_base_dir=_current_dir
        )

    def open_and_save_content(self, browser, url, ssl_key_file_path):
        return open_url_and_save_content(
            browser, url, ssl_key_file_path,
            data_base_dir=_current_dir
        )


if __name__ == "__main__":
    XCaptureEdgeAction.run_from_argv()
