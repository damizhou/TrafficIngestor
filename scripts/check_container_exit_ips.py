#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按单 CSV profile 创建临时 Docker 容器并查询公网出口 IPv4。

用法：
    python scripts/check_container_exit_ips.py

运行参数统一在本文件顶部的全局变量中修改。

脚本复用 profile 的镜像、网络、DNS 和固定 IP 配置，仅覆盖容器数量；
不挂载项目代码、不读取或修改 CSV 任务记录，也不执行采集任务。
查询完成后始终清理当前运行命名空间下的容器。
"""

from __future__ import annotations

import concurrent.futures
import ipaddress
import re
import sys
import tempfile
from dataclasses import replace
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "trafficIngestor"
for import_root in (PROJECT_ROOT, SOURCE_ROOT):
    root_text = str(import_root)
    if root_text not in sys.path:
        sys.path.insert(0, root_text)

from host_scheduler import single_csv_profiles as profile_runner
from host_scheduler.csv_ingestor_common import CsvIngestorProfile


PROFILE_PATH = PROJECT_ROOT / "trafficIngestor" / "single_csv" / "wiki_black_5.py"
CONTAINER_COUNT = 15
MOUNT_CODE_IN_CONTAINERS = False
DISABLE_OFFLOAD_DURING_POOL_PREPARE = False
DOCKER_COMMAND_TIMEOUT = 60
EXIT_IP_ENDPOINTS = (
    "https://api.ipify.org",
    "https://ifconfig.me/ip",
    "https://icanhazip.com",
)
EXIT_IP_REQUEST_TIMEOUT = 15
TEMPORARY_WORKSPACE_PREFIX = "traffic_ingestor_exit_ip_"
EXIT_IP_QUERY = f"""
for url in {' '.join(EXIT_IP_ENDPOINTS)}; do
    if command -v curl >/dev/null 2>&1; then
        value=$(curl -4 -fsS --max-time {EXIT_IP_REQUEST_TIMEOUT} "$url" 2>/dev/null || true)
    elif command -v wget >/dev/null 2>&1; then
        value=$(wget -qO- --timeout={EXIT_IP_REQUEST_TIMEOUT} "$url" 2>/dev/null || true)
    else
        exit 2
    fi
    if [ -n "$value" ]; then
        printf '%s\\n' "$value"
        exit 0
    fi
done
exit 1
""".strip()


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


def extract_ipv4(output: str) -> Optional[str]:
    candidates = re.findall(r"(?<!\d)(?:\d{1,3}\.){3}\d{1,3}(?!\d)", output or "")
    for candidate in candidates:
        try:
            address = ipaddress.ip_address(candidate)
        except ValueError:
            continue
        if address.version == 4:
            return str(address)
    return None


def compact_command_error(stdout: str, stderr: str) -> str:
    detail = " ".join(part.strip() for part in (stdout, stderr) if part and part.strip())
    return detail[:500] if detail else "无输出"


def query_container(
    ingestor,
    container: str,
    timeout: int,
) -> Tuple[str, Optional[str], Optional[str], str]:
    try:
        internal_result = ingestor.run_cmd(
            [
                "docker",
                "inspect",
                "-f",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}",
                container,
            ],
            timeout=timeout,
        )
    except Exception as exc:
        return container, None, None, f"读取容器地址异常: {exc}"
    internal_ip = extract_ipv4(internal_result.stdout)
    if internal_result.returncode != 0 or not internal_ip:
        error = compact_command_error(internal_result.stdout, internal_result.stderr)
        return container, None, None, f"读取容器地址失败: {error}"

    try:
        external_result = ingestor.run_cmd(
            ["docker", "exec", container, "sh", "-lc", EXIT_IP_QUERY],
            timeout=timeout,
        )
    except Exception as exc:
        return container, internal_ip, None, f"查询出口 IP 异常: {exc}"

    external_ip = extract_ipv4(external_result.stdout)
    if external_result.returncode != 0 or not external_ip:
        error = compact_command_error(external_result.stdout, external_result.stderr)
        return container, internal_ip, None, f"查询出口 IP 失败: {error}"
    return container, internal_ip, external_ip, ""


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
