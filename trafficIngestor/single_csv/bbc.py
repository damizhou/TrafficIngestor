"""默认 Chrome 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "BASE_DST": "/netdisk2/ww/bbc/white/20260821",
    "CONTAINER_COUNT": 450,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "bbc_white.csv"
    ),
}
ACTION_PROFILE = "tools/browsers/chrome.py"
