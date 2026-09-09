"""默认 Chrome 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT

CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "BASE_DST": "/netdisk/cl/20260817/us/edge",
    "CONTAINER_COUNT": 150,
    "DOCKER_IMAGE": "chuanzhoupan/trace_spider_edge:260309",
    "BROWSER_NAME": "edge",
    "BROWSER_VERSION_COMMANDS": (("microsoft-edge", "--version"),),
    "CSV_PATH": os.path.join(PROJECT_ROOT, "scripts", "result", "cl_url_us_edge.csv"),
}
ACTION_PROFILE = "tools/browsers/edge.py"
