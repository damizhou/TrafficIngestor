"""禁用 ML-KEM 的 Firefox 单 CSV 采集配置。"""

import os

from configs.single_csv._common import PROJECT_ROOT, RUN_UP_TO_FIVE


CONFIG = {
    "CONTAINER_COUNT": 15 * 20,
    "BASE_DST": "/netdisk/mlj/20260626/temp_disableML-KEM",
    "DOCKER_IMAGE": "chuanzhoupan/trace_spider_firefox:251104",
    "BROWSER_NAME": "firefox",
    "BROWSER_VERSION_COMMANDS": (("firefox", "--version"),),
    "RETRY": 5,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "urls_tls13_hybrid_top200_disable.csv",
    ),
}
RUN_POLICY = RUN_UP_TO_FIVE
RUNTIME_NAME = "traffic_capture_single_csv_firefox_disable"
ACTION_PROFILE = "firefox_disable"
