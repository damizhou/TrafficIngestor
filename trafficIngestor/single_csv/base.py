"""默认 Chrome 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "BASE_DST": "/netdisk/lhx/20261105/chrome",
    "CONTAINER_COUNT": 15,
    # 同一出口节点访问同一网站的最小间隔（秒）；不设置时默认 180，设为 0 关闭。
    # "SITE_COOLDOWN_SECONDS": 180,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "dailymirror-txt.csv",
    ),
}
ACTION_PROFILE = "tools/browsers/chrome.py"
