"""Bluesky 单 CSV 采集配置。"""

import os

from single_csv_configs._common import PROJECT_ROOT, RUN_UP_TO_FIVE


CONFIG = {
    "HOST_CODE_PATH": os.path.join(
        PROJECT_ROOT,
        "traffic_capture_single_csv_bsky",
    ),
    "BASE_DST": "/netdisk/mlj/urls_tls12_top100/20260605/",
    "RETRY": 5,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "small_tools",
        "result",
        "urls_tls12_top100.csv",
    ),
}
RUN_POLICY = RUN_UP_TO_FIVE
RUNTIME_NAME = "traffic_capture_single_csv_bsky"
ACTION_PROFILE = "chrome"
