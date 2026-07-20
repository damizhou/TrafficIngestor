"""欧洲固定节点 Chrome Clash 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "VPN_INFO_NAME": "vpns_info_europ",
    "CONTAINER_COUNT": 2 * 40,
    "BASE_DST": "/netdisk2/ww/trojan/top2000/homepage_only/260518/chrome/fra",
    "DELETE_INVALID_FILES_ON_FAIL": False,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "homeonly_merged_chrome_eu.csv",
    ),
}
RUNTIME_NAME = "traffic_capture_single_csv_fixed_ip_europe_clash"
ACTION_PROFILE = "tools/browsers/chrome.py"
