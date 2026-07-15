"""Rsia 固定 IP Chrome 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "SHARED_FIXED_IP_NETWORK": "traffic_ingestor_fixed_ip_rsia_net",
    "CONTAINER_COUNT": 2 * 40,
    "BASE_DST": "/netdisk2/ww/wiki/260514/chrome/sgp",
    "DOCKER_NETWORK": "traffic_ingestor_fixed_ip_rsia_net",
    "DOCKER_NETWORK_SUBNET_PREFIX": 23,
    "DOCKER_NETWORK_GATEWAY": "172.18.2.1",
    "CONTAINER_IP_START": "172.18.2.2",
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "wiki_edge.csv",
    ),
}
RUNTIME_NAME = "traffic_capture_single_csv_fixed_ip_rsia"
ACTION_PROFILE = "chrome"
