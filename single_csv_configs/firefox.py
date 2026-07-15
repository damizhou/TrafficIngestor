"""Firefox 单 CSV 采集配置。"""

import os

from single_csv_configs._common import PROJECT_ROOT, RUN_UP_TO_FIVE


CONFIG = {
    "HOST_CODE_PATH": os.path.join(
        PROJECT_ROOT,
        "traffic_capture_single_csv_firefox",
    ),
    "CONTAINER_COUNT": 15 * 20,
    "BASE_DST": "/netdisk/mlj/20260626/temp",
    "DOCKER_IMAGE": "chuanzhoupan/trace_spider_firefox:251104",
    "BROWSER_NAME": "firefox",
    "BROWSER_VERSION_COMMANDS": (("firefox", "--version"),),
    "RETRY": 5,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "small_tools",
        "result",
        "urls_tls13_hybrid_top200.csv",
    ),
}
RUN_POLICY = RUN_UP_TO_FIVE
RUNTIME_NAME = "traffic_capture_single_csv_firefox"
ACTION_PROFILE = "firefox"
