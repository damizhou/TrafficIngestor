"""临时 Chrome 单 CSV 采集配置。"""

import os

from single_csv_configs._common import PROJECT_ROOT, RUN_ONCE


CONFIG = {
    "BASE_DST": "/netdisk2/pcz/TrafficSimulation",
    "SUCCESS_DELETE_GUARD_FIELD": "url",
    "RETRY": 5,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "small_tools",
        "result",
        "wiki_temp.csv",
    ),
}
RUN_POLICY = RUN_ONCE
RUNTIME_NAME = "traffic_capture_single_csv_base_temp"
ACTION_PROFILE = "chrome"
