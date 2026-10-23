"""默认 Chrome 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "BASE_DST": "/netdisk/ww/github_cert/20261023/chrome",
    "CONTAINER_COUNT": 100,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "github_cert_3_chrome.csv",
    ),
}
ACTION_PROFILE = "tools/browsers/chrome.py"
