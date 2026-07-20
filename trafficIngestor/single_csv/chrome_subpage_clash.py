"""Chrome 子页面 Clash 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "BASE_DST": "/netdisk2/ww/trojan/top2000/subpages_merged",
    "DELETE_INVALID_FILES_ON_FAIL": False,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "subpages_merged.csv",
    ),
}
RUNTIME_NAME = "traffic_capture_single_csv_chrome_subpage_clash"
ACTION_PROFILE = "tools/browsers/chrome.py"
