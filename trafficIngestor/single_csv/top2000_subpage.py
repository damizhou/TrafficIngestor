"""Top 2000 子页面 Chrome 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "BASE_DST": "/netdisk/ww/top2000/subpages_merged",
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "subpages_merged.csv",
    ),
}
RUNTIME_NAME = "traffic_capture_single_csv_top2000_subpage"
ACTION_PROFILE = "tools/browsers/chrome.py"
