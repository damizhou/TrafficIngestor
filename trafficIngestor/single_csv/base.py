"""默认 Chrome 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "BASE_DST": "/netdisk/test",
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "noech_top10.csv",
    ),
}
RUNTIME_NAME = "traffic_capture_single_csv_base"
ACTION_PROFILE = "tools/browsers/chrome.py"
