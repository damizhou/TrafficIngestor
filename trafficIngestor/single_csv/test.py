"""默认 Chrome 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "BASE_DST": "/netdisk2/ww/test/20260909",
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "test1.csv",
    ),
    "DOCKER_NETWORK": "traffic_ingestor_fixed_ip_lon_net",
    "DOCKER_NETWORK_SUBNET_PREFIX": 22,
    "DOCKER_NETWORK_GATEWAY": "172.21.0.1",
    "CONTAINER_IP_START": "172.21.0.2",
}
ACTION_PROFILE = "tools/browsers/chrome.py"
