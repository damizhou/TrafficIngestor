"""Chrome 分组采集：同网站的 10 个页面共享一个 pcap，每组独立执行。

用法与其它单 CSV 配置一致：
    python trafficIngestor/host_scheduler/single_csv_profiles.py trafficIngestor/single_csv/batch.py
"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    # 打开按域名分组：同域名连续 10 行压成一组成员，共享 pcap 与 SSL keylog。
    "BATCH_BY_DOMAIN": True,
    "BATCH_GROUP_SIZE": 10,
    "DOCKER_IMAGE": "chuanzhoupan/trace_spider:250912",
    # 每个页面完整等待 120 秒再访问下一个，页面之间留 15 秒间隔。
    "BATCH_PAGE_WAIT_SECONDS": 120,
    "BATCH_PAGE_GAP_SECONDS": 15,
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "BASE_DST": "/netdisk/pcz/ablation_study/batch",
    "CONTAINER_COUNT": 120,
    # 一组 10 页约 23.5 分钟，另留浏览器启动与收尾时间；失败组只重试一次。
    "DOCKER_EXEC_TIMEOUT": 6000,
    "RETRY": 1,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT, "scripts", "result", "SiteBench_subpages_110site_120.csv"
    ),
}
ACTION_PROFILE = "tools/browsers/chrome.py"
