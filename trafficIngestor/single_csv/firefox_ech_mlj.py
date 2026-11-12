"""使用 Firefox 强制 ECH 采集 MLJ ECH 域名列表。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    # 保留输入 CSV，便于失败后重新执行或核对 40 轮任务。
    "DELETE_CSV_RECORD_ON_SUCCESS": False,
    # 按实际存储环境修改为本次采集的结果根目录。
    "BASE_DST": "/netdisk/mlj/20261111/ech",
    "DOCKER_IMAGE": "chuanzhoupan/trace_spider_firefox:152.0.6",
    "BROWSER_NAME": "firefox",
    "BROWSER_VERSION_COMMANDS": (("firefox", "--version"),),
    "CONTAINER_COUNT": 100,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "mlj",
        "ech-supported-hostnames_40.csv",
    ),
}

ACTION_PROFILE = "tools/browsers/firefox_ech.py"
