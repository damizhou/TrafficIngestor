"""单 CSV 配置文件共用的项目路径和运行策略。"""

from trafficIngestor.base_traffic_ingestor import BaseTrafficIngestor
from trafficIngestor.csv_ingestor_common import RunPolicy


PROJECT_ROOT = BaseTrafficIngestor.PROJECT_ROOT

RUN_ONCE = RunPolicy()
RUN_UP_TO_FIVE = RunPolicy(max_runs=5, stop_on_false=True)
RUN_UP_TO_FIVE_WITH_PENDING_WAIT = RunPolicy(
    max_runs=5,
    delay_seconds=1200,
    stop_on_false=True,
    require_pending_jobs=True,
)
