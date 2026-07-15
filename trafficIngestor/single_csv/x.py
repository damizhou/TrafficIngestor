"""X（Twitter）单 CSV 采集配置。"""

import os
from datetime import datetime

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "BASE_DST": (
        "/netdisk/x_with_ssl_key/"
        f"collection_without_login_{datetime.now().strftime('%y%m%d')}"
    ),
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "x_url_list.csv",
    ),
}
RUNTIME_NAME = "traffic_capture_single_csv_x"
ACTION_PROFILE = "tools/browsers/chrome.py"
