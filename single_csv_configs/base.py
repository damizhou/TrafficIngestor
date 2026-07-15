"""默认 Chrome 单 CSV 采集配置。"""

import os

from single_csv_configs._common import (
    PROJECT_ROOT,
    RUN_UP_TO_FIVE_WITH_PENDING_WAIT,
)


CONFIG = {
    "BASE_DST": "/netdisk/mlj/20260707/noech",
    "RESULT_DOMAIN_ROOT_DIR": "data",
    "TASK_CSV_DATA_ROOT_LAYOUT": True,
    "SUCCESS_DELETE_GUARD_FIELD": "url",
    "RETRY": 5,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "small_tools",
        "result",
        "noech_top10.csv",
    ),
}
RUN_POLICY = RUN_UP_TO_FIVE_WITH_PENDING_WAIT
RUNTIME_NAME = "traffic_capture_single_csv_base"
ACTION_PROFILE = "chrome"
