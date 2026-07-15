#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Clash 单 CSV 宿主入口配置。"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Type

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from trafficIngestor.base_traffic_ingestor import BaseTrafficIngestor
from trafficIngestor.csv_ingestor_common import (
    CsvIngestorProfile,
    RunPolicy,
    build_profile_ingestor,
    run_with_policy,
)
from trafficIngestor_clash.base_clash_traffic_ingestor import (
    BaseClashTrafficIngestor,
)


PROJECT_ROOT = BaseTrafficIngestor.PROJECT_ROOT
RUN_FIVE_WITH_HOURLY_WAIT = RunPolicy(
    max_runs=5,
    delay_seconds=3600,
    sleep_after_last_run=True,
)

# 普通 Clash 采集只需修改这个字典，然后直接运行本文件。
CONFIG = {
    "BASE_DST": "/netdisk2/ww/trojan/top2000/homepage_only_10/260605",
    "RETRY": 5,
    "DELETE_INVALID_FILES_ON_FAIL": False,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "homeonly_merged_10_clash.csv",
    ),
}
DEFAULT_PROFILE = "clash"

PROFILES = {
    "clash": CsvIngestorProfile(CONFIG, RUN_FIVE_WITH_HOURLY_WAIT),
    "chrome": CsvIngestorProfile(
        {
            "BASE_DST": "/netdisk2/ww/trojan/wiki/260413/chrome",
            "RETRY": 5,
            "DELETE_INVALID_FILES_ON_FAIL": False,
            "CSV_PATH": os.path.join(
                PROJECT_ROOT,
                "scripts",
                "result",
                "wiki_chrome.csv",
            ),
        },
        RUN_FIVE_WITH_HOURLY_WAIT,
    ),
    "chrome_subpage": CsvIngestorProfile(
        {
            "BASE_DST": "/netdisk2/ww/trojan/top2000/subpages_merged",
            "RETRY": 5,
            "DELETE_INVALID_FILES_ON_FAIL": False,
            "CSV_PATH": os.path.join(
                PROJECT_ROOT,
                "scripts",
                "result",
                "subpages_merged.csv",
            ),
        },
        RUN_FIVE_WITH_HOURLY_WAIT,
    ),
    "edge": CsvIngestorProfile(
        {
            "BASE_DST": "/netdisk2/ww/trojan/wiki/260413/edge",
            "DOCKER_IMAGE": "chuanzhoupan/trace_spider_edge:260309",
            "BROWSER_NAME": "edge",
            "BROWSER_VERSION_COMMANDS": (("microsoft-edge", "--version"),),
            "RETRY": 5,
            "CSV_PATH": os.path.join(
                PROJECT_ROOT,
                "scripts",
                "result",
                "wiki_edge.csv",
            ),
        },
        RUN_FIVE_WITH_HOURLY_WAIT,
    ),
    "firefox": CsvIngestorProfile(
        {
            "BASE_DST": "/netdisk2/ww/trojan/wiki/260413/firefox",
            "DOCKER_IMAGE": "chuanzhoupan/trace_spider_firefox:251104",
            "BROWSER_NAME": "firefox",
            "BROWSER_VERSION_COMMANDS": (("firefox", "--version"),),
            "RETRY": 5,
            "CSV_PATH": os.path.join(
                PROJECT_ROOT,
                "scripts",
                "result",
                "wiki_firefox.csv",
            ),
        },
        RUN_FIVE_WITH_HOURLY_WAIT,
    ),
    "fixed_ip_europe": CsvIngestorProfile(
        {
            "VPN_INFO_NAME": "vpns_info_europ",
            "CONTAINER_COUNT": 2 * 40,
            "BASE_DST": (
                "/netdisk2/ww/trojan/top2000/homepage_only/260518/chrome/fra"
            ),
            "RETRY": 5,
            "DELETE_INVALID_FILES_ON_FAIL": False,
            "CSV_PATH": os.path.join(
                PROJECT_ROOT,
                "scripts",
                "result",
                "homeonly_merged_chrome_eu.csv",
            ),
        },
        RUN_FIVE_WITH_HOURLY_WAIT,
    ),
    "fixed_ip_rsia": CsvIngestorProfile(
        {
            "VPN_INFO_NAME": "vpns_info_rsia",
            "CONTAINER_COUNT": 2 * 40,
            "BASE_DST": (
                "/netdisk2/ww/trojan/top2000/homepage_only/260518/chrome/sgp"
            ),
            "RETRY": 5,
            "DELETE_INVALID_FILES_ON_FAIL": False,
            "CSV_PATH": os.path.join(
                PROJECT_ROOT,
                "scripts",
                "result",
                "homeonly_merged_chrome_sgp.csv",
            ),
        },
        RUN_FIVE_WITH_HOURLY_WAIT,
    ),
}

RUNTIME_NAMES = {
    "clash": "traffic_capture_single_csv_clash",
    "chrome": "traffic_capture_single_csv_chrome_clash",
    "chrome_subpage": "traffic_capture_single_csv_chrome_subpage_clash",
    "edge": "traffic_capture_single_csv_edge_clash",
    "firefox": "traffic_capture_single_csv_firefox_clash",
    "fixed_ip_europe": "traffic_capture_single_csv_fixed_ip_europe_clash",
    "fixed_ip_rsia": "traffic_capture_single_csv_fixed_ip_rsia_clash",
}

ACTION_PROFILES = {
    "clash": "chrome_clash",
    "chrome": "chrome_clash",
    "chrome_subpage": "chrome_clash",
    "edge": "edge_clash",
    "firefox": "firefox_clash",
    "fixed_ip_europe": "chrome_clash",
    "fixed_ip_rsia": "chrome_clash",
}


def build_ingestor(
    profile_name: str,
    module_name: str,
    class_name: str = "TrafficIngestor",
) -> Type[BaseTrafficIngestor]:
    try:
        profile = PROFILES[profile_name]
    except KeyError as exc:
        raise ValueError(f"unknown Clash CSV profile: {profile_name}") from exc
    runtime_name = RUNTIME_NAMES[profile_name]
    profile_attributes = dict(profile.class_attributes)
    profile_attributes["RUNTIME_NAMESPACE"] = runtime_name
    profile_attributes["ACTION_PROFILE"] = ACTION_PROFILES[profile_name]
    configured_profile = CsvIngestorProfile(
        profile_attributes,
        profile.run_policy,
    )
    return build_profile_ingestor(
        BaseClashTrafficIngestor,
        class_name,
        module_name,
        profile_name,
        runtime_name,
        configured_profile,
    )


def run_profile(
    profile_name: str,
    ingestor_class: Type[BaseTrafficIngestor],
) -> None:
    try:
        policy = PROFILES[profile_name].run_policy
    except KeyError as exc:
        raise ValueError(f"unknown Clash CSV profile: {profile_name}") from exc
    run_with_policy(ingestor_class, policy)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a Clash single-CSV trafficIngestor capture. "
            f"The default profile is {DEFAULT_PROFILE!r}."
        )
    )
    parser.add_argument(
        "profile",
        nargs="?",
        default=DEFAULT_PROFILE,
        choices=sorted(PROFILES),
        help=(
            "Optional preset name. Omit it to run DEFAULT_PROFILE configured "
            "in this file."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    ingestor_class = build_ingestor(args.profile, __name__)
    run_profile(args.profile, ingestor_class)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
