"""
traffic_capture_single_csv_ech/action.py - force Chrome ECH capture.
"""
import os
import sys

_current_dir: str = os.path.dirname(os.path.abspath(__file__))
if _current_dir not in sys.path:
    sys.path.insert(0, _current_dir)
_app_root: str = _current_dir

from tools.base_action import BaseAction
from tools.chrome_ech import (
    create_chrome_driver,
    kill_chrome_processes,
    open_url_and_save_content,
    save_chrome_ech_evidence,
    validate_chrome_ech_result,
)


def _env_flag(name, default=False):
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_value(name):
    return os.environ.get(name, "").strip() or None


class EchCaptureAction(BaseAction):
    """Chrome capture action that rejects non-ECH visits."""

    browser_name = "Chrome ECH"
    pcap_lowest_size = 100000
    ssl_key_lowest_size = 128
    require_wireshark_decryptable = _env_flag("CHROME_ECH_REQUIRE_DECRYPTABLE", True)
    delete_invalid_files_on_fail = _env_flag("CHROME_ECH_DELETE_INVALID_FILES", False)

    def __init__(self):
        super().__init__()
        self._last_ech_validation_error = ""
        self._ech_extra_result_paths = {}

    def kill_browser_processes(self):
        kill_chrome_processes()

    def create_browser_driver(self, formatted_time, row_id, artifact_label=None):
        return create_chrome_driver(
            self.allowed_domain,
            formatted_time,
            f"{row_id}",
            data_base_dir=_app_root,
            proxy_server=self.get_browser_proxy_server(),
            proxy_bypass_list=self.get_browser_proxy_bypass_list(),
            logger=self.logger,
            artifact_label=artifact_label,
            chrome_binary_path=_env_value("CHROME_ECH_BINARY_PATH"),
            chromedriver_path=_env_value("CHROME_ECH_CHROMEDRIVER_PATH"),
            force_ech=True,
        )

    def open_and_save_content(self, browser, url, ssl_key_file_path):
        return open_url_and_save_content(
            browser,
            url,
            ssl_key_file_path,
            data_base_dir=_app_root,
            logger=self.logger,
        )

    def validate_files(self, pcap_path, ssl_key_file_path, content_path, html_path):
        self._last_ech_validation_error = ""
        self._ech_extra_result_paths = {}
        if not super().validate_files(pcap_path, ssl_key_file_path, content_path, html_path):
            return False

        ok, detail = validate_chrome_ech_result(
            self.allowed_domain,
            pcap_path=pcap_path,
            ssl_key_path=ssl_key_file_path,
            require_wireshark_decryptable=self.require_wireshark_decryptable,
        )
        if ok:
            evidence_dir = os.path.join(_app_root, "ech_evidence")
            self._ech_extra_result_paths = save_chrome_ech_evidence(
                self.allowed_domain,
                pcap_path,
                evidence_dir,
                ssl_key_path=ssl_key_file_path,
            )
            self.logger.info(f"ECH校验通过: {detail}")
            return True

        self._last_ech_validation_error = detail
        self.logger.warning(f"ECH校验失败: {detail}")
        return False

    def build_failure_details(self, open_url_error, page_not_found, path_diagnostics):
        details = super().build_failure_details(open_url_error, page_not_found, path_diagnostics)
        if self._last_ech_validation_error:
            details.append(f"ech_invalid={self._last_ech_validation_error[:300]}")
        return details

    def write_result(self, meta_path, result):
        if result.get("success") and self._ech_extra_result_paths:
            result.update(self._ech_extra_result_paths)
        super().write_result(meta_path, result)


if __name__ == "__main__":
    EchCaptureAction.run_from_argv()
