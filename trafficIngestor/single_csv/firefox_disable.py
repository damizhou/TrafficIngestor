"""禁用 ML-KEM 的 Firefox 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "CONTAINER_COUNT": 15 * 20,
    "BASE_DST": "/netdisk/mlj/20260626/temp_disableML-KEM",
    "DOCKER_IMAGE": "chuanzhoupan/trace_spider_firefox:152.0.6",
    "BROWSER_NAME": "firefox",
    "BROWSER_VERSION_COMMANDS": (("firefox", "--version"),),
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "urls_tls13_hybrid_top200_disable.csv",
    ),
}
RUNTIME_NAME = "traffic_capture_single_csv_firefox_disable"
ACTION_PROFILE = "tools/browsers/firefox_disable.py"
