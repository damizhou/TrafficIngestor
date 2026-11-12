"""使用 Firefox 强制关闭 ECH 采集 MLJ 对照域名列表。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": False,
    "BASE_DST": "/netdisk/mlj/20261111/noech",
    "DOCKER_IMAGE": "chuanzhoupan/trace_spider_firefox:152.0.6",
    "BROWSER_NAME": "firefox",
    "BROWSER_VERSION_COMMANDS": (("firefox", "--version"),),
    "CONTAINER_COUNT": 100,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "mlj",
        "noech-supported-hostnames_40.csv",
    ),
}

ACTION_PROFILE = "tools/browsers/firefox_noech.py"
