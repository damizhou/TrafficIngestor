#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
news_receiver_traffic.py

从数据库读取新闻 URL，使用容器池并发采集流量数据。
"""

import os
import sys
import configparser
import time
import threading
from typing import List, Dict, Tuple

from sqlalchemy import create_engine, text

# 添加项目根目录到路径
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from trafficIngestor.base_traffic_ingestor import BaseTrafficIngestor, get_real_username


class _DailyErrorLogWriter:
    """按天切分错误日志文件。"""

    def __init__(self, logs_dir: str, file_prefix: str):
        self._logs_dir = logs_dir
        self._file_prefix = file_prefix
        self._lock = threading.Lock()
        self._fp = None
        self._current_day = ""
        self._current_path = ""
        os.makedirs(self._logs_dir, exist_ok=True)

    def _rotate_if_needed(self) -> None:
        day = time.strftime("%Y%m%d")
        if self._fp is not None and day == self._current_day:
            return

        if self._fp is not None:
            try:
                self._fp.flush()
            finally:
                self._fp.close()

        self._current_day = day
        self._current_path = os.path.join(self._logs_dir, f"{self._file_prefix}_{day}.log")
        self._fp = open(self._current_path, "a", encoding="utf-8", buffering=1)

    @property
    def path(self) -> str:
        with self._lock:
            if not self._current_path:
                self._rotate_if_needed()
            return self._current_path

    def write(self, message: str) -> None:
        line = (message or "").rstrip("\n")
        if not line:
            return

        with self._lock:
            self._rotate_if_needed()
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            self._fp.write(f"[{ts}] {line}\n")
            self._fp.flush()

    def close(self) -> None:
        with self._lock:
            if self._fp is None:
                return
            try:
                self._fp.flush()
            finally:
                self._fp.close()
                self._fp = None


class TrafficIngestor(BaseTrafficIngestor):
    """新闻流量采集器"""

    # ============== 配置 ==============
    BASE_NAME = 'traffic_capture_single_db'
    CONTAINER_PREFIX = f"{get_real_username()}_{BASE_NAME}"
    # 默认并发不宜过高；可通过环境变量覆盖（例如 180）
    CONTAINER_COUNT = 15 * 10
    HOST_CODE_PATH = os.path.join(_project_root, BASE_NAME)
    BASE_DST = '/netdisk/news_receiver'
    DOCKER_IMAGE = "chuanzhoupan/trace_spider:250912"
    RETRY = 5
    BATCH_SIZE = 20000
    CLEAR_HOST_CODE_SUBDIRS_AFTER_BATCH = False
    FINAL_FAIL_TRAFFIC_STATUS = -1

    # 需要处理的表及其对应的 domain
    TABLES_CONFIG = [
        {"table": "dailymail_content", "domain": "dailymail.co.uk"},
        # {"table": "bbc_content", "domain": "bbc.com"},
        # {"table": "nih_content", "domain": "nih.gov"},
        # {"table": "forbeschina_content", "domain": "forbeschina.com"},
    ]

    TABLE_MAP = {
        "bbc.com": "bbc_content",
        "nih.gov": "nih_content",
        "forbeschina.com": "forbeschina_content",
        "dailymail.co.uk": "dailymail_content",
    }

    DB_CONFIG_PATH = os.path.join(_project_root, 'db', 'db_config.ini')
    ERROR_LOG_PREFIX = "traffic_capture_single_db_error"

    FINAL_FAILURE_KEYWORDS = (
        " -> give up",     # BaseTrafficIngestor 在重试耗尽后输出
        "任务最终失败",       # 本文件 on_task_failed 中的最终失败标记
    )

    ERROR_KEYWORDS = (
        "warn:",
        "fatal:",
        "error:",
        "异常",
    )

    def __init__(self):
        super().__init__()
        self._db_engine = None
        logs_dir = os.path.join(_current_dir, "logs")
        self._error_log_writer = _DailyErrorLogWriter(logs_dir, self.ERROR_LOG_PREFIX)

    @classmethod
    def _is_error_message(cls, msg: str) -> bool:
        msg_lc = msg.lower()
        if any(k in msg_lc for k in cls.FINAL_FAILURE_KEYWORDS):
            return True
        return any(k in msg_lc for k in cls.ERROR_KEYWORDS)

    def log(self, *args) -> None:
        super().log(*args)
        msg = " ".join(str(x) for x in args)
        if self._is_error_message(msg):
            self._error_log_writer.write(msg)

    def setup(self) -> None:
        """初始化数据库连接"""
        try:
            self._db_engine = self._connect_db()
            self.log("数据库连接成功")
            self.log(f"错误日志文件(按天): {self._error_log_writer.path}")
        except Exception as e:
            self.log(f"FATAL: 数据库连接失败，无法继续: {e}")
            sys.exit(1)

    def cleanup(self) -> None:
        self._error_log_writer.close()
        if self._db_engine is not None:
            try:
                self._db_engine.dispose()
            except Exception as e:
                self.log(f"WARN: 关闭数据库连接池异常: {e}")
            finally:
                self._db_engine = None

    def exec_once(self, task: Dict[str, str]) -> Tuple[bool, str]:
        ok, err = super().exec_once(task)
        if ok:
            return True, err

        url = str(task.get("url", "")).strip()
        if url and "url=" not in err:
            return False, f"url={url} | {err}"
        return False, err

    def _connect_db(self):
        """连接数据库并返回引擎"""
        cp = configparser.ConfigParser(interpolation=None)
        if not cp.read(self.DB_CONFIG_PATH, encoding="utf-8-sig"):
            raise FileNotFoundError(f"未找到配置文件：{self.DB_CONFIG_PATH}")
        if not cp.has_section("mysql"):
            raise KeyError("缺少配置节 [mysql]")

        c = cp["mysql"]

        def need(k):
            v = c.get(k, "").strip()
            if not v:
                raise ValueError(f"缺少 {k}")
            return v

        user = need("user")
        pwd = need("password")
        host = need("host")
        port = need("port")
        db = need("database")
        url = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}"

        cs = c.get("charset", "").strip()
        if cs:
            url += f"?charset={cs}"

        engine = create_engine(url, pool_pre_ping=True, future=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine

    def fetch_jobs(self) -> List[Dict[str, str]]:
        """从数据库获取任务"""
        all_jobs: List[Dict[str, str]] = []

        for table_config in self.TABLES_CONFIG:
            table = table_config["table"]
            domain = table_config["domain"]

            sql = f"""
                SELECT id, url
                FROM {table}
                WHERE (pcap_path IS NULL OR pcap_path = '')
                AND (traffic_status IS NULL OR traffic_status <> :final_fail_status)
                AND url IS NOT NULL AND url <> ''
                ORDER BY id
                LIMIT {self.BATCH_SIZE}
            """

            try:
                table_jobs: List[Dict[str, str]] = []
                with self._db_engine.connect() as conn:
                    result = conn.execute(text(sql), {"final_fail_status": self.FINAL_FAIL_TRAFFIC_STATUS})
                    for row in result:
                        row_id = str(row[0])
                        url = row[1]
                        table_jobs.append({"row_id": row_id, "url": url, "domain": domain})

                if table_jobs:
                    self.log(f"从 {table} 获取了 {len(table_jobs)} 条任务")
                    all_jobs.extend(table_jobs)
            except Exception as e:
                self.log(f"WARN: 从数据库获取任务失败: {e}")

        return all_jobs

    def on_task_success(self, task: Dict[str, str], paths: Dict[str, str]) -> None:
        """任务成功后更新数据库"""
        try:
            row_id = int(task.get("row_id", "0"))
            domain = task.get("domain", "")
            table = self.TABLE_MAP.get(domain, "")

            if not table:
                self.log(f"WARN: 不支持的 domain: {domain}，跳过数据库上传")
                return

            sql = f"""
                UPDATE {table}
                SET classify_status=%s,
                    traffic_status=%s,
                    pcap_path=%s,
                    ssl_key_path=%s,
                    content_path=%s,
                    html_path=%s,
                    traffic_feature=%s,
                    current_url=%s
                WHERE id=%s AND (pcap_path IS NULL OR pcap_path = '')
            """.strip()

            data = (0, 0, paths['pcap'], paths['ssl_key'], paths['content'], paths['html'], None, paths.get('current_url', ''), row_id)

            conn = self._db_engine.raw_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, data)
                    affected = cur.rowcount
                conn.commit()
                if affected <= 0:
                    raise RuntimeError(f"数据库更新无匹配行 row_id={row_id}")
                self.log(f"数据库更新成功 row_id={row_id}")
            finally:
                conn.close()

        except Exception as e:
            self.log(f"WARN: 数据库操作异常 row_id={task.get('row_id', '')}: {e}")
            raise

    def on_task_failed(self, task: Dict[str, str], error: str) -> None:
        """任务失败后标记数据库"""
        try:
            row_id = int(task.get("row_id", "0"))
            domain = task.get("domain", "")
            url = task.get("url", "")
            table = self.TABLE_MAP.get(domain, "")

            self.log(f"ERROR: 任务最终失败 row_id={row_id} url={url} err={error[:200]}")

            if not table:
                return

            sql = f"""
                UPDATE {table}
                SET traffic_status=%s
                WHERE id=%s AND (pcap_path IS NULL OR pcap_path = '')
            """.strip()

            conn = self._db_engine.raw_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, (self.FINAL_FAIL_TRAFFIC_STATUS, row_id))
                    affected = cur.rowcount
                conn.commit()
                if affected > 0:
                    self.log(f"失败记录已标记到数据库 row_id={row_id} url={url}")
                else:
                    self.log(f"WARN: 失败记录未写入（无匹配行） row_id={row_id} url={url}")
            finally:
                conn.close()

        except Exception as e:
            self.log(f"WARN: 数据库标记异常 row_id={task.get('row_id', '')} url={task.get('url', '')}: {e}")


if __name__ == "__main__":
    TrafficIngestor.main()
