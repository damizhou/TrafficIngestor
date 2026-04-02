"""
traffic_capture_x/action.py - X(Twitter)流量捕获
继承 BaseAction
"""
import os
import socket
import sys

# 添加当前目录到路径（容器内 /app 包含 tools/ 子目录）
_current_dir = os.path.dirname(os.path.abspath(__file__))
if _current_dir not in sys.path:
    sys.path.insert(0, _current_dir)

from tools.base_action import BaseAction
from tools.chrome import open_url_and_save_content


class XCaptureAction(BaseAction):
    """流量捕获 Action"""

    pcap_lowest_size = 100000
    ssl_key_lowest_size = 128
    clash_proxy_port = 7890
    clash_log_tail_lines = 20
    delete_invalid_files_on_fail = os.environ.get("DELETE_INVALID_FILES_ON_FAIL", "1").strip().lower() not in {
        "0",
        "false",
        "no",
    }

    def get_browser_proxy_server(self):
        return "http://127.0.0.1:7890"

    def get_browser_proxy_bypass_list(self):
        return "127.0.0.1;localhost;::1"

    def get_browser_startup_settle_seconds(self):
        return 2.0

    def use_task_scoped_logger(self):
        return True

    def get_clash_runtime_dir(self):
        if self.container_name:
            return os.path.join(_current_dir, "clash_runtime", self.container_name)
        return os.path.join(_current_dir, "clash_runtime")

    @staticmethod
    def _get_file_size(path):
        try:
            if path and os.path.exists(path):
                return os.path.getsize(path)
        except OSError:
            pass
        return -1

    @staticmethod
    def _read_text_tail(path, max_lines):
        if not path or not os.path.exists(path):
            return ""
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
        except OSError:
            return ""
        return "".join(lines[-max_lines:]).strip()

    def log_clash_runtime_diagnostics(self):
        if self.logger is None:
            return

        runtime_dir = self.get_clash_runtime_dir()
        start_log_path = os.path.join(runtime_dir, "clash.start.log")
        outer_keylog_path = os.path.join(runtime_dir, "trojan_outer_sslkey.log")
        clash_log_path = os.path.join(_current_dir, "clash-for-linux", "logs", "clash.log")

        proxy_port_open = False
        proxy_port_error = ""
        try:
            sock = socket.create_connection(("127.0.0.1", self.clash_proxy_port), timeout=1)
            sock.close()
            proxy_port_open = True
        except OSError as e:
            proxy_port_error = f"{type(e).__name__}: {e}"

        self.logger.error(
            "Clash 失败诊断: "
            f"runtime_dir={runtime_dir} | proxy_port_open={proxy_port_open} | "
            f"proxy_port_error={proxy_port_error or 'none'} | "
            f"clash_log_size={self._get_file_size(clash_log_path)} | "
            f"start_log_size={self._get_file_size(start_log_path)} | "
            f"outer_keylog_size={self._get_file_size(outer_keylog_path)}"
        )

        for label, path in (
            ("clash.start.log", start_log_path),
            ("clash.log", clash_log_path),
        ):
            tail_text = self._read_text_tail(path, self.clash_log_tail_lines)
            if tail_text:
                self.logger.error(f"Clash 日志尾部[{label}] path={path}\n{tail_text}")

    def open_and_save_content(self, browser, url, ssl_key_file_path):
        try:
            return open_url_and_save_content(
                browser,
                url,
                ssl_key_file_path,
                data_base_dir=_current_dir,
                logger=self.logger,
            )
        except Exception:
            self.log_clash_runtime_diagnostics()
            raise


if __name__ == "__main__":
    XCaptureAction.run_from_argv()
