"""X（Twitter）单 CSV 采集配置。"""

import os
from datetime import datetime

from configs.single_csv._common import PROJECT_ROOT, RUN_UP_TO_FIVE


CONFIG = {
    "BASE_DST": (
        "/netdisk/x_with_ssl_key/"
        f"collection_without_login_{datetime.now().strftime('%y%m%d')}"
    ),
    "RETRY": 5,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "x_url_list.csv",
    ),
}
RUN_POLICY = RUN_UP_TO_FIVE
RUNTIME_NAME = "traffic_capture_single_csv_x"
ACTION_PROFILE = "chrome"
