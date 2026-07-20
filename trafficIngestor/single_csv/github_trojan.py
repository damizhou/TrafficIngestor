"""GitHub 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "BASE_DST": "/netdisk2/ww/trojan/github/20260720",
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "github_repose_1000_trojan.csv",
    ),
}
RUNTIME_NAME = "github_traffic_trojan"
ACTION_PROFILE = "tools/browsers/chrome.py"
