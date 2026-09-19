"""默认 Chrome 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "DELETE_CSV_FROM_JOURNAL_ON_START": True,
    "CLEAN_AND_RECREATE_CONTAINERS": True,
    "DOCKER_IMAGE": "chuanzhoupan/trace_spider:250912",
    "CONTAINER_COUNT": 300,
    "BASE_DST": "/netdisk/yjn/20260914/us/chrome",
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "top15000_50_yjn.csv",
    ),
}
ACTION_PROFILE = "tools/browsers/chrome.py"
