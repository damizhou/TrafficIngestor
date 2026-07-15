"""Threads 单 CSV 采集配置。"""

import os

from configs.single_csv._common import (
    PROJECT_ROOT,
    RUN_UP_TO_FIVE_WITH_PENDING_WAIT,
)


CONFIG = {
    "BASE_DST": "/netdisk/mlj/urls_tls13_traditional_top100/20260605/",
    "RETRY": 5,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "urls_tls13_traditional_top100.csv",
    ),
}
RUN_POLICY = RUN_UP_TO_FIVE_WITH_PENDING_WAIT
RUNTIME_NAME = "traffic_capture_single_csv_threads"
ACTION_PROFILE = "chrome"
