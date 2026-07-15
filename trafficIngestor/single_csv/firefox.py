"""Firefox 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "CONTAINER_COUNT": 15 * 20,
    "BASE_DST": "/netdisk/mlj/20260626/temp",
    "DOCKER_IMAGE": "chuanzhoupan/trace_spider_firefox:251104",
    "BROWSER_NAME": "firefox",
    "BROWSER_VERSION_COMMANDS": (("firefox", "--version"),),
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "urls_tls13_hybrid_top200.csv",
    ),
}
RUNTIME_NAME = "traffic_capture_single_csv_firefox"
ACTION_PROFILE = "firefox"
