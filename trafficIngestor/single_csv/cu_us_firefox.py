"""默认 Chrome 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT



CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "BASE_DST": "/netdisk/cl/20260817/us/firefox",
    "CONTAINER_COUNT": 150,
    "DOCKER_IMAGE": "chuanzhoupan/trace_spider_firefox:152.0.6",
    "BROWSER_NAME": "firefox",
    "BROWSER_VERSION_COMMANDS": (("firefox", "--version"),),
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "cl_url_us_firefox.csv",
    ),
}
ACTION_PROFILE = "tools/browsers/firefox.py"
