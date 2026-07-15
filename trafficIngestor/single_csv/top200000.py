"""Top 200000 Chrome 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "BASE_DST": "/netdisk/ww/top200000",
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "top300000_ingestor.csv",
    ),
}
RUNTIME_NAME = "traffic_capture_single_csv_top200000"
ACTION_PROFILE = "chrome"
