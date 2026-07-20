"""默认 Chrome Clash 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "BASE_DST": "/netdisk2/ww/trojan/top2000/homepage_only_10/260605",
    "DELETE_INVALID_FILES_ON_FAIL": False,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "homeonly_merged_10_clash.csv",
    ),
}
RUNTIME_NAME = "traffic_capture_single_csv_clash"
ACTION_PROFILE = "tools/browsers/chrome.py"
