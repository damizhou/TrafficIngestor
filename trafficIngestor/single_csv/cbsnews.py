"""默认 Chrome 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "BASE_DST": "/netdisk2/ww/cbsnews/20261112/",
    "CONTAINER_COUNT": 100,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "ww",
        "cbsnews.com.csv"
    ),
}
ACTION_PROFILE = "tools/browsers/chrome.py"
