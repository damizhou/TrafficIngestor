"""默认 Chrome 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "DOCKER_IMAGE": "chuanzhoupan/trace_spider:250912",
    "CONTAINER_COUNT": 150,
    "BASE_DST": "/netdisk/cl/20260817/us/chrome",
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "cl_url_us_chrome.csv",
    ),
}
ACTION_PROFILE = "tools/browsers/chrome.py"
