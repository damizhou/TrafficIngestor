#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""宿主机侧单次访问：docker exec 调用容器内 visit_page.py，解析结果行。"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from typing import Any, Dict

from urlFind import config
from urlFind.pool import UrlFindPool, parse_marker_line
from urlFind.protocol import CONTAINER_WORKER_SCRIPT, OUTCOME_ERROR, OUTCOME_TIMEOUT

# 宿主机侧 subprocess 超时 = 容器内硬超时 + 余量；正常情况下由容器内 timeout 先结束
HOST_TIMEOUT_MARGIN_SECONDS = 60
# coreutils timeout 超时时的退出码
TIMEOUT_EXIT_CODES = {124, 137}


@dataclass
class VisitReport:
    result: Dict[str, Any]
    # 基础设施错误（docker exec 失败、没有结果行、宿主机超时），计入容器健康度
    infra_error: bool


def _error_result(visit_id: str, url: str, outcome: str, reason: str) -> Dict[str, Any]:
    return {
        "visit_id": visit_id, "request_url": url, "final_url": "", "outcome": outcome,
        "http_status": None, "reason": reason, "links": [], "elapsed": 0.0,
    }


def run_visit(pool: UrlFindPool, container: str, visit_id: str, url: str) -> VisitReport:
    payload = {
        "visit_id": visit_id,
        "url": url,
        "page_load_timeout": config.PAGE_LOAD_TIMEOUT_SECONDS,
        "settle_seconds": config.PAGE_SETTLE_SECONDS,
        "max_links": config.MAX_LINKS_PER_PAGE,
    }
    cmd = [
        "docker", "exec", "-w", "/tmp", container,
        "timeout", "-k", "10", str(config.VISIT_TIMEOUT_SECONDS),
        config.CONTAINER_PYTHON, "-u", CONTAINER_WORKER_SCRIPT,
        json.dumps(payload, ensure_ascii=False),
    ]
    try:
        cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                            timeout=config.VISIT_TIMEOUT_SECONDS + HOST_TIMEOUT_MARGIN_SECONDS)
    except subprocess.TimeoutExpired:
        pool.kill_browsers(container)
        return VisitReport(_error_result(visit_id, url, OUTCOME_TIMEOUT, "host_timeout"), True)
    except OSError as exc:
        return VisitReport(_error_result(visit_id, url, OUTCOME_ERROR, f"docker_exec:{exc}"), True)

    result = parse_marker_line(cp.stdout)
    if result is not None and result.get("visit_id") == visit_id:
        return VisitReport(result, False)
    if cp.returncode in TIMEOUT_EXIT_CODES:
        # 容器内硬超时：页面拖垮了整次访问，属于站点问题，不计入容器健康度
        pool.kill_browsers(container)
        return VisitReport(_error_result(visit_id, url, OUTCOME_TIMEOUT, "visit_timeout"), False)
    tail = (cp.stderr or cp.stdout or "").strip().splitlines()
    detail = tail[-1][:300] if tail else ""
    return VisitReport(
        _error_result(visit_id, url, OUTCOME_ERROR, f"no_result:rc={cp.returncode}:{detail}"), True)
