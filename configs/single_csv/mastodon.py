"""Mastodon 单 CSV 采集配置。"""

import os

from configs.single_csv._common import (
    PROJECT_ROOT,
    RUN_UP_TO_FIVE_WITH_PENDING_WAIT,
)


CONFIG = {
    "BASE_DST": "/netdisk/mlj/urls_tls13_hybrid_top100/20260617/",
    "RETRY": 5,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "urls_tls13_hybrid_top100.csv",
    ),
}
RUN_POLICY = RUN_UP_TO_FIVE_WITH_PENDING_WAIT
RUNTIME_NAME = "traffic_capture_single_csv_mastodon"
ACTION_PROFILE = "chrome"
