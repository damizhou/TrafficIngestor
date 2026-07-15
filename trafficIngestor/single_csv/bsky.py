"""Bluesky 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "BASE_DST": "/netdisk/mlj/urls_tls12_top100/20260605/",
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "urls_tls12_top100.csv",
    ),
}
RUNTIME_NAME = "traffic_capture_single_csv_bsky"
ACTION_PROFILE = "tools/browsers/chrome.py"
