#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""配置驱动的单 CSV 浏览器流量采集容器入口。"""

from __future__ import annotations

import importlib
import os
import socket
import sys
from dataclasses import dataclass
from types import ModuleType


_current_dir = os.path.dirname(os.path.abspath(__file__))
if _current_dir not in sys.path:
    sys.path.insert(0, _current_dir)

from tools.base_action import BaseAction


@dataclass(frozen=True)
class ActionProfile:
    backend: str
    browser_name: str
    use_clash_proxy: bool = False
    enable_clash_diagnostics: bool = False


ACTION_PROFILES = {
    "chrome": ActionProfile("chrome", "Chrome"),
    "chrome_clash": ActionProfile(
        "chrome",
        "Chrome",
        use_clash_proxy=True,
        enable_clash_diagnostics=True,
    ),
    "edge": ActionProfile("edge", "Edge"),
    "edge_clash": ActionProfile("edge", "Edge", use_clash_proxy=True),
    "firefox": ActionProfile("firefox", "Firefox"),
    "firefox_disable": ActionProfile("firefox_disable", "Firefox"),
    "firefox_clash": ActionProfile("firefox", "Firefox", use_clash_proxy=True),
}


def load_action_profile() -> ActionProfile:
    profile_name = os.environ.get("TRAFFIC_ACTION_PROFILE", "chrome").strip().lower()
    try:
        return ACTION_PROFILES[profile_name]
    except KeyError as exc:
        choices = ", ".join(sorted(ACTION_PROFILES))
        raise RuntimeError(
            f"unknown TRAFFIC_ACTION_PROFILE={profile_name!r}; choices={choices}"
        ) from exc


def read_env_bool(name: str, default: bool) -> bool:
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() not in {"0", "false", "no"}


PROFILE = load_action_profile()


class ConfiguredCaptureAction(BaseAction):
    """根据宿主 profile 选择浏览器后端和 Clash 行为。"""

    pcap_lowest_size = 100000
    ssl_key_lowest_size = 128
    browser_name = PROFILE.browser_name
    clash_proxy_port = 7890
    clash_log_tail_lines = 20
    delete_invalid_files_on_fail = (
        read_env_bool("DELETE_INVALID_FILES_ON_FAIL", True)
        if PROFILE.enable_clash_diagnostics
        else True
    )

    def get_backend_module(self) -> ModuleType:
        return importlib.import_module(f"tools.{PROFILE.backend}")

    def kill_browser_processes(self):
        backend = self.get_backend_module()
        if PROFILE.backend == "chrome":
            backend.kill_chrome_processes()
            return
        if PROFILE.backend == "edge":
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

        if PROFILE.backend == "chrome":
            return backend.create_chrome_driver(
                *task_args,
                logger=self.logger,
                **common_kwargs,
            )
        if PROFILE.backend == "edge":
            return backend.create_edge_driver(
                *task_args,
                blocked_hosts=self.get_capture_exclude_hosts(),
                **common_kwargs,
            )
        return backend.create_firefox_driver(*task_args, **common_kwargs)

    def open_and_save_content(self, browser, url, ssl_key_file_path):
        backend = self.get_backend_module()
        kwargs = {"data_base_dir": _current_dir}
        if PROFILE.backend == "chrome":
            kwargs["logger"] = self.logger

        try:
            return backend.open_url_and_save_content(
                browser,
                url,
                ssl_key_file_path,
                **kwargs,
            )
        except Exception:
            if PROFILE.enable_clash_diagnostics:
                self.log_clash_runtime_diagnostics()
            raise

    def get_capture_exclude_hosts(self):
        backend = self.get_backend_module()
        if PROFILE.backend == "edge":
            return backend.get_edge_background_capture_exclude_hosts(
                self.allowed_domain
            )
        if PROFILE.backend in {"firefox", "firefox_disable"}:
            return backend.get_firefox_background_capture_exclude_hosts(
                self.allowed_domain
            )
        return ()

    def get_browser_proxy_server(self):
        if PROFILE.use_clash_proxy:
            return "http://127.0.0.1:7890"
        return None

    def get_browser_proxy_bypass_list(self):
        if PROFILE.use_clash_proxy:
            return "127.0.0.1;localhost;::1"
        return None

    def get_browser_startup_settle_seconds(self):
        if PROFILE.enable_clash_diagnostics:
            return 2.0
        return 0.0

    def use_task_scoped_logger(self):
        return PROFILE.enable_clash_diagnostics

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
            _current_dir,
            "clash-for-linux",
            "logs",
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
