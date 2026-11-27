#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按单 CSV profile 创建临时 Docker 容器并查询公网出口 IPv4。

用法：
    python scripts/code/check_container_exit_ips.py

运行参数统一在本文件顶部的全局变量中修改。

脚本复用 profile 的镜像、网络、DNS 和固定 IP 配置，仅覆盖容器数量；
不挂载项目代码、不读取或修改 CSV 任务记录，也不执行采集任务。
查询完成后始终清理当前运行命名空间下的容器。
"""

from __future__ import annotations

import concurrent.futures
import sys
import tempfile
from dataclasses import replace
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = PROJECT_ROOT / "trafficIngestor"
for import_root in (PROJECT_ROOT, SOURCE_ROOT):
    root_text = str(import_root)
    if root_text not in sys.path:
        sys.path.insert(0, root_text)

from host_scheduler import single_csv_profiles as profile_runner
from host_scheduler.csv_ingestor_common import CsvIngestorProfile
from host_scheduler.exit_ip_probe import query_container


PROFILE_PATH = PROJECT_ROOT / "trafficIngestor" / "single_csv" / "test.py"
CONTAINER_COUNT = 1
MOUNT_CODE_IN_CONTAINERS = False
DISABLE_OFFLOAD_DURING_POOL_PREPARE = False
DOCKER_COMMAND_TIMEOUT = 60
TEMPORARY_WORKSPACE_PREFIX = "traffic_ingestor_exit_ip_"


def resolve_profile_path(raw_path: Path) -> Path:
    if raw_path.is_absolute():
        return raw_path.resolve()

    candidates = ((Path.cwd() / raw_path).resolve(), (PROJECT_ROOT / raw_path).resolve())
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return candidates[0]


def build_probe_class(profile_path: Path, count: int, workspace: Path):
    definition = profile_runner.load_profile_definition(profile_path)
    attributes = dict(definition.profile.class_attributes)
    attributes.update(
        {
            "CONTAINER_COUNT": count,
            "BASE_DST": str(workspace / "output"),
            "MOUNT_CODE_IN_CONTAINERS": MOUNT_CODE_IN_CONTAINERS,
            "DISABLE_OFFLOAD_DURING_POOL_PREPARE": (
                DISABLE_OFFLOAD_DURING_POOL_PREPARE
            ),
        }
    )
    definition = replace(
        definition,
        profile=CsvIngestorProfile(attributes, definition.profile.run_policy),
    )
    return definition, profile_runner.build_ingestor(
        definition,
        profile_runner.__name__,
    )


def print_results(
    names: Iterable[str],
    results: Dict[str, Tuple[Optional[str], Optional[str], str]],
) -> bool:
    failed = False
    print("container\tinternal_ipv4\texternal_ipv4\tstatus", flush=True)
    for name in names:
        internal_ip, external_ip, error = results[name]
        if error:
            failed = True
            print(
                f"{name}\t{internal_ip or '-'}\t{external_ip or '-'}\t{error}",
                flush=True,
            )
        else:
            print(f"{name}\t{internal_ip}\t{external_ip}\tOK", flush=True)
    return not failed


def run() -> int:
    if CONTAINER_COUNT <= 0:
        raise ValueError("CONTAINER_COUNT 必须是正整数")
    if DOCKER_COMMAND_TIMEOUT <= 0:
        raise ValueError("DOCKER_COMMAND_TIMEOUT 必须是正整数")

    profile_path = resolve_profile_path(PROFILE_PATH)
    if not profile_path.is_file():
        raise FileNotFoundError(f"profile 文件不存在: {profile_path}")

    with tempfile.TemporaryDirectory(prefix=TEMPORARY_WORKSPACE_PREFIX) as temp_dir:
        workspace = Path(temp_dir)
        definition, ingestor_class = build_probe_class(
            profile_path,
            CONTAINER_COUNT,
            workspace,
        )
        ingestor = ingestor_class()
        lock_acquired = False
        names = []
        try:
            ingestor.acquire_runtime_lock()
            lock_acquired = True
            names = ingestor.prepare_pool_once()
            print(
                f"profile={definition.source_path} count={len(names)} "
                f"image={ingestor.DOCKER_IMAGE} network={ingestor.get_target_docker_network()}",
                flush=True,
            )
            results: Dict[str, Tuple[Optional[str], Optional[str], str]] = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(names)) as pool:
                future_map = {
                    pool.submit(
                        query_container,
                        ingestor,
                        name,
                        DOCKER_COMMAND_TIMEOUT,
                    ): name
                    for name in names
                }
                for future in concurrent.futures.as_completed(future_map):
                    container, internal_ip, external_ip, error = future.result()
                    results[container] = (internal_ip, external_ip, error)
            return 0 if print_results(names, results) else 1
        finally:
            try:
                ingestor.remove_containers()
            finally:
                if lock_acquired:
                    ingestor.release_runtime_lock()


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
