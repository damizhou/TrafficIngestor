"""Top 2000 首页 Chrome 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "CONTAINER_COUNT": 300,
    "BASE_DST": "/netdisk2/ww/top2000/homepage_only/20260724",
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "top4000_ingestor_10.csv",
    ),
}
ACTION_PROFILE = "tools/browsers/chrome.py"
