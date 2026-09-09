"""默认 Chrome 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "BASE_DST": "/netdisk2/ww/top2000/homepage_only_5/20260903",
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "homeonly_merged_5.csv",
    ),
}
ACTION_PROFILE = "tools/browsers/chrome.py"
