"""默认 Chrome Clash 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "BASE_DST": "/netdisk2/ww/trojan/top2000/homepage_only_5/20260812",
    "DELETE_INVALID_FILES_ON_FAIL": False,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "homeonly_merged_5_clash.csv",
    ),
}
ACTION_PROFILE = "tools/browsers/chrome.py"
