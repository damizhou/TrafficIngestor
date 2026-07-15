"""Threads 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "BASE_DST": "/netdisk/mlj/urls_tls13_traditional_top100/20260605/",
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "urls_tls13_traditional_top100.csv",
    ),
}
RUNTIME_NAME = "traffic_capture_single_csv_threads"
ACTION_PROFILE = "tools/browsers/chrome.py"
