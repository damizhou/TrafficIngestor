#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""配置驱动的单 CSV 浏览器流量采集容器入口。"""

from __future__ import annotations

import importlib
import os
import socket
import sys
from pathlib import PurePosixPath
from types import ModuleType


_current_dir = os.path.dirname(os.path.abspath(__file__))
if _current_dir not in sys.path:
    sys.path.insert(0, _current_dir)

from tools.base_action import BaseAction


def validate_browser_module_path(module_path: str) -> str:
    """校验并返回可直接导入的浏览器模块相对路径。"""
    normalized_path = module_path.strip().replace("\\", "/")
    path = PurePosixPath(normalized_path)
    if (
        path.is_absolute()
        or path.suffix != ".py"
        or path.parts[:2] != ("tools", "browsers")
        or ".." in path.parts
    ):
        raise RuntimeError(
            "TRAFFIC_ACTION_PROFILE must be a Python file under tools/browsers: "
            f"{module_path!r}"
        )
    return normalized_path


def infer_backend_kind(backend: ModuleType) -> str:
    """按模块实际暴露的驱动接口推导浏览器类型。"""
    if callable(getattr(backend, "create_chrome_driver", None)):
        return "chrome"
    if callable(getattr(backend, "create_edge_driver", None)):
        return "edge"
    if callable(getattr(backend, "create_firefox_driver", None)):
        return "firefox"
    raise RuntimeError(
        "browser module must expose create_chrome_driver, create_edge_driver, "
        "or create_firefox_driver"
    )


def read_env_bool(name: str, default: bool) -> bool:
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() not in {"0", "false", "no"}


BROWSER_MODULE_PATH = validate_browser_module_path(
    os.environ.get(
        "TRAFFIC_ACTION_PROFILE",
        "tools/browsers/chrome.py",
    )
)
USE_CLASH_PROXY = read_env_bool("TRAFFIC_USE_CLASH_PROXY", False)
ENABLE_CLASH_DIAGNOSTICS = read_env_bool(
    "TRAFFIC_ENABLE_CLASH_DIAGNOSTICS",
    False,
)
BACKEND_MODULE = importlib.import_module(
    BROWSER_MODULE_PATH.removesuffix(".py").replace("/", ".")
)
BACKEND_KIND = infer_backend_kind(BACKEND_MODULE)
BROWSER_NAME = BACKEND_KIND.capitalize()


class ConfiguredCaptureAction(BaseAction):
    """根据宿主 profile 选择浏览器后端和 Clash 行为。"""

    pcap_lowest_size = 100000
    ssl_key_lowest_size = 128
    browser_name = BROWSER_NAME
    clash_proxy_port = 7890
    clash_log_tail_lines = 20
    delete_invalid_files_on_fail = read_env_bool(
        "DELETE_INVALID_FILES_ON_FAIL",
        True,
    )

    def get_backend_module(self) -> ModuleType:
        return BACKEND_MODULE

    def kill_browser_processes(self):
        backend = self.get_backend_module()
        if BACKEND_KIND == "chrome":
            backend.kill_chrome_processes()
            return
        if BACKEND_KIND == "edge":
            backend.kill_edge_processes()
            return
        backend.kill_firefox_processes()

    def create_browser_driver(self, formatted_time, row_id, artifact_label=None):
        backend = self.get_backend_module()
        common_kwargs = {
            "data_base_dir": _current_dir,
            "proxy_server": self.get_browser_proxy_server(),
            "proxy_bypass_list": self.get_browser_proxy_bypass_list(),
            "artifact_label": artifact_label,
        }
        task_args = (self.allowed_domain, formatted_time, f"{row_id}")

        if BACKEND_KIND == "chrome":
            return backend.create_chrome_driver(
                *task_args,
                logger=self.logger,
                **common_kwargs,
            )
        if BACKEND_KIND == "edge":
            return backend.create_edge_driver(
                *task_args,
                blocked_hosts=self.get_capture_exclude_hosts(),
                **common_kwargs,
            )
        return backend.create_firefox_driver(*task_args, **common_kwargs)

    def open_and_save_content(self, browser, url, ssl_key_file_path):
        backend = self.get_backend_module()
        kwargs = {"data_base_dir": _current_dir}
        if BACKEND_KIND == "chrome":
            kwargs["logger"] = self.logger

        try:
            return backend.open_url_and_save_content(
                browser,
                url,
                ssl_key_file_path,
                **kwargs,
            )
        except Exception:
            if ENABLE_CLASH_DIAGNOSTICS:
                self.log_clash_runtime_diagnostics()
            raise

    def get_capture_exclude_hosts(self):
        backend = self.get_backend_module()
        if BACKEND_KIND == "edge":
            return backend.get_edge_background_capture_exclude_hosts(
                self.allowed_domain
            )
        if BACKEND_KIND == "firefox":
            return backend.get_firefox_background_capture_exclude_hosts(
                self.allowed_domain
            )
        return ()

    def validate_files(self, pcap_path, ssl_key_file_path, content_path, html_path):
        self._last_backend_validation_error = ""
        base_valid = super().validate_files(
            pcap_path,
            ssl_key_file_path,
            content_path,
            html_path,
        )

        backend = self.get_backend_module()
        validator = getattr(backend, "validate_firefox_ech_key_log", None)
        if validator is None:
            return base_valid

        summarizer = getattr(backend, "summarize_firefox_ech_key_log", None)
        try:
            if summarizer is not None:
                counts = summarizer(ssl_key_file_path)
                self.logger.info(
                    "Firefox ECH keylog labels: "
                    f"ECH_SECRET={counts.get('ECH_SECRET', 0)}, "
                    f"ECH_CONFIG={counts.get('ECH_CONFIG', 0)}"
                )
            valid, message = validator(ssl_key_file_path)
        except (OSError, ValueError) as exc:
            valid = False
            message = f"ech_keylog_validation_error={type(exc).__name__}: {exc}"

        if valid:
            self.logger.info(message)
            return base_valid

        self._last_backend_validation_error = message
        self.logger.error(f"Firefox ECH 密钥日志校验失败: {message}")
        return False

    def build_failure_details(self, open_url_error, page_not_found, path_diagnostics):
        reasons = super().build_failure_details(
            open_url_error,
            page_not_found,
            path_diagnostics,
        )
        backend_error = getattr(self, "_last_backend_validation_error", "")
        if backend_error:
            reasons.append(f"backend_validation={backend_error[:300]}")
        return reasons

    def get_browser_proxy_server(self):
        if USE_CLASH_PROXY:
            return "http://127.0.0.1:7890"
        return None

    def get_browser_proxy_bypass_list(self):
        if USE_CLASH_PROXY:
            return "127.0.0.1;localhost;::1"
        return None

    def get_browser_startup_settle_seconds(self):
        if ENABLE_CLASH_DIAGNOSTICS:
            return 2.0
        return 0.0

    def use_task_scoped_logger(self):
        return ENABLE_CLASH_DIAGNOSTICS

    def get_clash_runtime_dir(self):
        if self.container_name:
            return os.path.join(_current_dir, "clash_runtime", self.container_name)
        return os.path.join(_current_dir, "clash_runtime")

    @staticmethod
    def get_file_size(path):
        try:
            if path and os.path.exists(path):
                return os.path.getsize(path)
        except OSError:
            pass
        return -1

    @staticmethod
    def read_text_tail(path, max_lines):
        if not path or not os.path.exists(path):
            return ""
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as file:
                lines = file.readlines()
        except OSError:
            return ""
        return "".join(lines[-max_lines:]).strip()

    def log_clash_runtime_diagnostics(self):
        if self.logger is None:
            return

        runtime_dir = self.get_clash_runtime_dir()
        start_log_path = os.path.join(runtime_dir, "clash.start.log")
        outer_keylog_path = os.path.join(runtime_dir, "trojan_outer_sslkey.log")
        clash_log_path = os.path.join(
            runtime_dir,
            "clash.log",
        )

        proxy_port_open = False
        proxy_port_error = ""
        try:
            sock = socket.create_connection(
                ("127.0.0.1", self.clash_proxy_port),
                timeout=1,
            )
            sock.close()
            proxy_port_open = True
        except OSError as exc:
            proxy_port_error = f"{type(exc).__name__}: {exc}"

        self.logger.error(
            "Clash 失败诊断: "
            f"runtime_dir={runtime_dir} | proxy_port_open={proxy_port_open} | "
            f"proxy_port_error={proxy_port_error or 'none'} | "
            f"clash_log_size={self.get_file_size(clash_log_path)} | "
            f"start_log_size={self.get_file_size(start_log_path)} | "
            f"outer_keylog_size={self.get_file_size(outer_keylog_path)}"
        )

        for label, path in (
            ("clash.start.log", start_log_path),
            ("clash.log", clash_log_path),
        ):
            tail_text = self.read_text_tail(path, self.clash_log_tail_lines)
            if tail_text:
                self.logger.error(
                    f"Clash 日志尾部[{label}] path={path}\n{tail_text}"
                )


if __name__ == "__main__":
    ConfiguredCaptureAction.run_from_argv()
