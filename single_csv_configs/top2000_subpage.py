"""Top 2000 子页面 Chrome 单 CSV 采集配置。"""

import os

from single_csv_configs._common import PROJECT_ROOT, RUN_ONCE


CONFIG = {
    "HOST_CODE_PATH": os.path.join(
        PROJECT_ROOT,
        "traffic_capture_single_csv_top2000_subpage",
    ),
    "BASE_DST": "/netdisk/ww/top2000/subpages_merged",
    "RETRY": 5,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "small_tools",
        "subpages_merged.csv",
    ),
}
RUN_POLICY = RUN_ONCE
RUNTIME_NAME = "traffic_capture_single_csv_top2000_subpage"
ACTION_PROFILE = "chrome"
