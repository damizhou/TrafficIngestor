from datetime import datetime
import logging
import logging.handlers
import os
from typing import Optional

from utils import project_path

LOG_NAME = "url_list_collector"
LOG_FORMAT = "%(asctime)s - %(levelname)s - [%(container)s] - %(message)s"
DEFAULT_CONTAINER = "unknown"

_current_container = DEFAULT_CONTAINER
_file_handler: Optional[logging.Handler] = None


def _sanitize_container_name(container: Optional[str]) -> str:
    """规范化容器名，避免非法文件名字符。"""
    raw = (container or DEFAULT_CONTAINER).strip() or DEFAULT_CONTAINER
    return "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in raw)


def _safe_chown(path: str) -> None:
    """在容器内尽量修正日志目录/文件权限，失败时忽略。"""
    try:
        uid = os.geteuid()
        gid = os.getegid()
    except Exception:
        uid = int(os.getenv("HOST_UID") or 1002)
        gid = int(os.getenv("HOST_GID") or 1002)
    try:
        os.chown(path, uid, gid)
    except Exception:
        pass


class _ContainerFilter(logging.Filter):
    """为日志记录补充 container 字段。"""

    def filter(self, record: logging.LogRecord) -> bool:
        record.container = _sanitize_container_name(getattr(record, "container", None) or _current_container)
        return True


def _build_log_file_path(container: str) -> str:
    logs_dir = os.path.join(project_path, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    _safe_chown(logs_dir)

    today = datetime.now().strftime("%Y%m%d")
    filename = f"{container}_{today}.log"
    return os.path.join(logs_dir, filename)


def setup_logging() -> logging.Logger:
    """初始化 logger（控制台输出），文件输出由 set_log_container 绑定。"""
    logger = logging.getLogger(LOG_NAME)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    if logger.handlers:
        return logger

    formatter = logging.Formatter(LOG_FORMAT)
    container_filter = _ContainerFilter()

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(container_filter)
    logger.addHandler(console_handler)

    return logger


def set_log_container(container: str) -> None:
    """切换当前进程的日志文件到指定容器。"""
    global _current_container, _file_handler

    _current_container = _sanitize_container_name(container)
    logger = logging.getLogger(LOG_NAME)

    if _file_handler is not None:
        logger.removeHandler(_file_handler)
        try:
            _file_handler.close()
        except Exception:
            pass
        _file_handler = None

    log_file = _build_log_file_path(_current_container)
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=100 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    file_handler.addFilter(_ContainerFilter())
    logger.addHandler(file_handler)
    _file_handler = file_handler
    _safe_chown(log_file)


logger = setup_logging()
