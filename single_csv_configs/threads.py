"""Threads 单 CSV 采集配置。"""

import os

from single_csv_configs._common import (
    PROJECT_ROOT,
    RUN_UP_TO_FIVE_WITH_PENDING_WAIT,
)


CONFIG = {
    "HOST_CODE_PATH": os.path.join(
        PROJECT_ROOT,
        "traffic_capture_single_csv_threads",
    ),
    "BASE_DST": "/netdisk/mlj/urls_tls13_traditional_top100/20260605/",
    "RETRY": 5,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "small_tools",
        "result",
        "urls_tls13_traditional_top100.csv",
    ),
}
RUN_POLICY = RUN_UP_TO_FIVE_WITH_PENDING_WAIT
RUNTIME_NAME = "traffic_capture_single_csv_threads"
ACTION_PROFILE = "chrome"
