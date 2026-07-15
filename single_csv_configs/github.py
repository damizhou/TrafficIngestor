"""GitHub 单 CSV 采集配置。"""

import os

from single_csv_configs._common import PROJECT_ROOT, RUN_ONCE


CONFIG = {
    "BASE_DST": "/netdisk/github_with_ssl_key/lyl",
    "RETRY": 5,
    "CSV_PATH": os.path.join(PROJECT_ROOT, "db", "github_repos_1000.csv"),
}
RUN_POLICY = RUN_ONCE
RUNTIME_NAME = "github_traffic"
ACTION_PROFILE = "chrome"
