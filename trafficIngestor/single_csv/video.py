"""Chrome 视频采集配置：走 chrome_video.py（consent 处理 + 视频起播 + 4 分钟采集窗口）。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "BASE_DST": "/netdisk/lhx/20261104/chrome_video",
    "CONTAINER_COUNT": 10,
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "dailymirror-video.csv",
    ),
}
ACTION_PROFILE = "tools/browsers/chrome_video.py"