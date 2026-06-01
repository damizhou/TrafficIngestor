"""
Ungoogled Chromium browser driver module.

This module reuses the Chrome capture helpers and only changes binary/driver
resolution plus process cleanup. It is intended for containers where Google
Chrome has been replaced by ungoogled Chromium.
"""
import os
import shutil
import subprocess
import sys
from pathlib import Path

_current_dir: str = os.path.dirname(os.path.abspath(__file__))
_project_root: str = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from tools.chrome import (
    add_cookies,
    build_browser_error_diagnostics,
    create_chrome_driver,
    open_url_and_save_content,
    screenshot_full_page,
)


UNGOOGLED_CHROMIUM_BINARY_ENV_KEYS = (
    "UNGOOGLED_CHROMIUM_BINARY",
    "CHROMIUM_BINARY",
)
UNGOOGLED_CHROMEDRIVER_ENV_KEYS = (
    "UNGOOGLED_CHROMEDRIVER",
    "CHROMEDRIVER",
    "CHROMEWEBDRIVER",
)
UNGOOGLED_CHROMIUM_BINARY_NAMES = (
    "ungoogled-chromium",
    "chromium",
    "chromium-browser",
)
UNGOOGLED_CHROMEDRIVER_NAMES = (
    "chromedriver",
)
UNGOOGLED_CHROMIUM_COMMON_PATHS = (
    "/usr/bin/ungoogled-chromium",
    "/usr/local/bin/ungoogled-chromium",
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser",
    "/opt/ungoogled-chromium/ungoogled-chromium",
    "/opt/ungoogled-chromium/chromium",
    "/opt/ungoogled-chromium/chrome",
)
UNGOOGLED_CHROMEDRIVER_COMMON_PATHS = (
    "/usr/local/bin/chromedriver",
    "/usr/bin/chromedriver",
    "/opt/ungoogled-chromium/chromedriver",
)


def _resolve_from_candidates(env_keys, executable_names, common_paths):
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


def resolve_ungoogled_chromium_binary():
    return _resolve_from_candidates(
        UNGOOGLED_CHROMIUM_BINARY_ENV_KEYS,
        UNGOOGLED_CHROMIUM_BINARY_NAMES,
        UNGOOGLED_CHROMIUM_COMMON_PATHS,
    )


def resolve_ungoogled_chromedriver():
    return _resolve_from_candidates(
        UNGOOGLED_CHROMEDRIVER_ENV_KEYS,
        UNGOOGLED_CHROMEDRIVER_NAMES,
        UNGOOGLED_CHROMEDRIVER_COMMON_PATHS,
    )


def create_ungoogled_chromium_driver(
    task_name=None,
    formatted_time=None,
    parsers=None,
    enable_ssl_key_log=True,
    data_base_dir=None,
    proxy_server=None,
    proxy_bypass_list=None,
    logger=None,
    blocked_hosts=None,
    chromium_binary_path=None,
    chromedriver_path=None,
):
    """Create a WebDriver instance backed by ungoogled Chromium."""
    resolved_binary = chromium_binary_path or resolve_ungoogled_chromium_binary()
    if not resolved_binary:
        raise FileNotFoundError(
            "Ungoogled Chromium binary not found. Set UNGOOGLED_CHROMIUM_BINARY "
            "or install it at a known path."
        )

    resolved_driver = chromedriver_path or resolve_ungoogled_chromedriver()
    effective_blocked_hosts = () if blocked_hosts is None else blocked_hosts

    return create_chrome_driver(
        task_name=task_name,
        formatted_time=formatted_time,
        parsers=parsers,
        enable_ssl_key_log=enable_ssl_key_log,
        data_base_dir=data_base_dir,
        proxy_server=proxy_server,
        proxy_bypass_list=proxy_bypass_list,
        logger=logger,
        blocked_hosts=effective_blocked_hosts,
        chrome_binary_path=resolved_binary,
        chromedriver_path=resolved_driver,
    )


def kill_ungoogled_chromium_processes():
    patterns = {
        "chromedriver",
        "ungoogled-chromium",
        "chromium",
        "chromium-browser",
    }
    env_binary = os.environ.get("UNGOOGLED_CHROMIUM_BINARY")
    if env_binary:
        binary_name = Path(env_binary).name
        if binary_name:
            patterns.add(binary_name)

    for pattern in sorted(patterns):
        try:
            subprocess.run(
                ["pkill", "-f", pattern],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception as e:
            print(f"Error killing ungoogled Chromium process pattern {pattern}: {e}")


__all__ = [
    "add_cookies",
    "build_browser_error_diagnostics",
    "create_ungoogled_chromium_driver",
    "kill_ungoogled_chromium_processes",
    "open_url_and_save_content",
    "resolve_ungoogled_chromedriver",
    "resolve_ungoogled_chromium_binary",
    "screenshot_full_page",
]
