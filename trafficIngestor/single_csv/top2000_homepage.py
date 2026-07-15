"""Top 2000 首页 Chrome 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "BASE_DST": "/netdisk/ww/top2000/homepage_only",
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "homeonly_merged.csv",
    ),
}
RUNTIME_NAME = "traffic_capture_single_csv_top2000_homepage"
ACTION_PROFILE = "chrome"
