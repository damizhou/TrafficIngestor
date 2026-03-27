#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
base_traffic_ingestor.py

流量采集器基类，封装 Docker 容器池管理、任务调度、文件处理等通用逻辑。
各具体采集脚本继承此基类，只需配置参数和实现差异部分。
"""

from __future__ import annotations
import csv
import ipaddress
import os
import sys
import time
import json
import signal
import queue
import subprocess
import shutil
import threading
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, List, Dict, Tuple, Any
from concurrent.futures import ThreadPoolExecutor

from tqdm import tqdm

# 添加项目根目录到路径
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def get_real_username() -> str:
    """获取真实用户名，即使在 sudo 下也能获取原始用户"""
    return os.environ.get('SUDO_USER') or os.environ.get('USER') or os.getlogin()


class BaseTrafficIngestor(ABC):
    """
    流量采集器基类

    子类需要：
    1. 设置类属性（配置参数）
    2. 实现 fetch_jobs() 方法获取任务
    3. 可选：覆盖 process_result() 方法自定义结果处理
    4. 可选：覆盖 on_task_success() / on_task_failed() 方法
    """

    # ============== 子类必须设置的配置 ==============
    CONTAINER_PREFIX: str = ""
    HOST_CODE_PATH: str = ""
    BASE_DST: str = ""

    # ============== 可选配置（有默认值）==============
    CONTAINER_COUNT: int = 3
    DOCKER_IMAGE: str = "chuanzhoupan/trace_spider:250912"
    CONTAINER_CODE_PATH: str = "/app"
    CREATE_WITH_TTY: bool = True
    DOCKER_EXEC_TIMEOUT: int = 6000
    RETRY: int = 5
    FIRST_EXEC_INTERVAL: float = 1.0
    DOCKER_NETWORK: Optional[str] = None
    CONTAINER_IP_START: Optional[str] = None
    DEFAULT_UID: int = int(os.environ.get('SUDO_UID', os.getuid()))
    DEFAULT_GID: int = int(os.environ.get('SUDO_GID', os.getgid()))
    CLEAR_HOST_CODE_SUBDIRS_AFTER_BATCH: bool = True

    def __init__(self):
        self._stats_lock = threading.Lock()
        self._csv_lock = threading.Lock()
        self._pbar = None
        self._first_exec_lock = threading.Lock()
        self._first_exec_next_ts = 0.0
        self._first_exec_done_containers = set()
        self._runtime_prepared = False

        # 全局统计
        self._global_start_time = 0.0
        self._global_ok = 0
        self._global_fail = 0
        self._global_container_count = max(int(self.CONTAINER_COUNT), 1)

    # ============== 日志 ==============
    def log(self, *args) -> None:
        """打印带时间戳的日志，适配进度条"""
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        msg = f"[{ts}] " + " ".join(str(x) for x in args)
        if self._pbar is not None:
            tqdm.write(msg)
        else:
            print(msg, flush=True)

    # ============== 命令执行 ==============
    def run_cmd(self, cmd: List[str], timeout: Optional[int] = None) -> subprocess.CompletedProcess:
        """执行命令并返回结果"""
        return subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, timeout=timeout
        )

    # ============== Docker 容器管理 ==============
    def ensure_docker_available(self) -> None:
        """确保 Docker 可用"""
        try:
            self.run_cmd(["docker", "version"]).check_returncode()
        except Exception as e:
            self.log("FATAL: docker 不可用。", e)
            sys.exit(2)

    def container_exists(self, name: str) -> Optional[bool]:
        """检查容器是否存在，返回 None 表示不存在"""
        cp = self.run_cmd(["docker", "inspect", "-f", "{{.State.Running}}", name])
        if cp.returncode != 0:
            return None
        out = cp.stdout.strip().lower()
        return (out == "true") or (out == "false")

    def container_running(self, name: str) -> bool:
        """检查容器是否正在运行"""
        cp = self.run_cmd(["docker", "inspect", "-f", "{{.State.Running}}", name])
        return (cp.returncode == 0) and (cp.stdout.strip().lower() == "true")

    def build_container_ip(self, index: int) -> Optional[str]:
        """按容器序号生成固定 IP；未配置时返回 None。"""
        if not self.CONTAINER_IP_START:
            return None
        try:
            return str(ipaddress.IPv4Address(self.CONTAINER_IP_START) + index)
        except ValueError as e:
            self.log(f"FATAL: 非法的容器起始 IP：{self.CONTAINER_IP_START} -> {e}")
            sys.exit(2)

    def get_target_docker_network(self) -> str:
        """返回固定 IP 所在的目标 Docker 网络名。"""
        return self.DOCKER_NETWORK or "bridge"

    def inspect_target_network(self) -> Tuple[List[ipaddress.IPv4Network], Dict[str, str]]:
        """读取目标 Docker 网络上的 IPv4 子网与已分配地址。"""
        network_name = self.get_target_docker_network()
        cp = self.run_cmd(["docker", "network", "inspect", network_name])
        if cp.returncode != 0:
            detail = (cp.stderr or cp.stdout).strip() or "unknown error"
            self.log(f"WARN: 无法读取 Docker 网络 {network_name}：{detail}")
            return [], {}

        try:
            payload = json.loads(cp.stdout)
            network_info = payload[0] if payload else {}
        except (json.JSONDecodeError, IndexError, TypeError) as e:
            self.log(f"WARN: 解析 Docker 网络 {network_name} 失败：{e}")
            return [], {}

        subnets: List[ipaddress.IPv4Network] = []
        for item in network_info.get("IPAM", {}).get("Config", []):
            subnet = str(item.get("Subnet", "")).strip()
            if not subnet:
                continue
            try:
                subnets.append(ipaddress.ip_network(subnet, strict=False))
            except ValueError:
                self.log(f"WARN: 忽略非法 Docker 子网配置：{subnet}")

        allocated: Dict[str, str] = {}
        for container in (network_info.get("Containers") or {}).values():
            name = str(container.get("Name", "")).strip()
            ipv4 = str(container.get("IPv4Address", "")).split("/", 1)[0].strip()
            if not name or not ipv4:
                continue
            try:
                ipaddress.IPv4Address(ipv4)
            except ValueError:
                continue
            allocated[name] = ipv4

        return subnets, allocated

    def is_usable_container_ip(
        self,
        candidate: ipaddress.IPv4Address,
        subnets: List[ipaddress.IPv4Network]
    ) -> bool:
        """判断候选 IP 是否落在目标子网内且不是网络/广播地址。"""
        if not subnets:
            return True
        for subnet in subnets:
            if candidate not in subnet:
                continue
            if candidate == subnet.network_address:
                return False
            if subnet.num_addresses > 1 and candidate == subnet.broadcast_address:
                return False
            return True
        return False

    def build_container_specs(self, names: List[str]) -> List[Tuple[int, str, Optional[str]]]:
        """为容器池分配固定 IP，自动跳过已占用或不可用地址。"""
        specs = [(index, name, self.build_container_ip(index)) for index, name in enumerate(names)]
        if not specs or specs[0][2] is None:
            return specs

        try:
            start_ip = ipaddress.IPv4Address(self.CONTAINER_IP_START)
        except ValueError as e:
            self.log(f"FATAL: 非法的容器起始 IP：{self.CONTAINER_IP_START} -> {e}")
            sys.exit(2)

        subnets, allocated = self.inspect_target_network()
        used_ips = {
            ipaddress.IPv4Address(ip)
            for ip in allocated.values()
        }
        assigned: Dict[str, str] = {}
        reserved_ips: List[ipaddress.IPv4Address] = []

        for name in names:
            current_ip = allocated.get(name)
            if not current_ip:
                continue
            current_addr = ipaddress.IPv4Address(current_ip)
            if current_addr < start_ip:
                continue
            if not self.is_usable_container_ip(current_addr, subnets):
                continue
            assigned[name] = current_ip
            reserved_ips.append(current_addr)

        next_ip = start_ip
        if reserved_ips:
            next_ip = ipaddress.IPv4Address(max(int(ip) for ip in reserved_ips) + 1)

        for _, name, _ in specs:
            if name in assigned:
                continue
            while next_ip in used_ips or not self.is_usable_container_ip(next_ip, subnets):
                next_ip += 1
            assigned[name] = str(next_ip)
            used_ips.add(next_ip)
            next_ip += 1

        resolved_specs = [(index, name, assigned[name]) for index, name, _ in specs]
        if resolved_specs and resolved_specs[0][2] != specs[0][2]:
            self.log(
                f"WARN: 固定 IP 自动顺延，起始配置={specs[0][2]}，"
                f"实际首个可用地址={resolved_specs[0][2]}"
            )
        return resolved_specs

    def get_container_ipv4(self, name: str) -> Optional[str]:
        """获取容器当前 IPv4 地址。"""
        if self.DOCKER_NETWORK:
            inspect_expr = f'{{{{with index .NetworkSettings.Networks "{self.DOCKER_NETWORK}"}}}}{{{{.IPAddress}}}}{{{{end}}}}'
        else:
            inspect_expr = "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}"
        cp = self.run_cmd(["docker", "inspect", "-f", inspect_expr, name])
        if cp.returncode != 0:
            return None
        values = cp.stdout.strip().split()
        return values[0] if values else None

    def remove_container(self, name: str) -> None:
        """删除单个容器。"""
        cp = self.run_cmd(["docker", "rm", "-f", name])
        if cp.returncode != 0:
            self.log(f"FATAL: 删除容器失败: {name} -> {cp.stderr.strip()}")
            sys.exit(2)
        self.log(f"removed container: {name}")

    def create_container(
        self,
        name: str,
        host_code_path: str,
        image: str,
        container_ip: Optional[str] = None
    ) -> None:
        """创建容器，同时挂载代码目录和 tools 目录"""
        uid, gid = str(os.getuid()), str(os.getgid())
        tools_path = os.path.join(_project_root, 'tools')
        self.log(f"creating container: {name}")
        cmd = [
            "docker", "run",
            "--init",
            "--dns", "172.17.0.1",  # 使用宿主机 dnsmasq 作为 DNS 缓存
            "--volume", f"{host_code_path}:{self.CONTAINER_CODE_PATH}",
            "--volume", f"{tools_path}:{self.CONTAINER_CODE_PATH}/tools",
            "-e", f"HOST_UID={uid}",
            "-e", f"HOST_GID={gid}",
            "--privileged",
        ]
        if self.DOCKER_NETWORK:
            cmd += ["--network", self.DOCKER_NETWORK]
        if container_ip:
            cmd += ["--ip", container_ip]
        if self.CREATE_WITH_TTY:
            cmd += ["-itd"]
        else:
            cmd += ["-d"]
        cmd += ["--name", name, image, "/bin/bash"]
        cp = self.run_cmd(cmd)
        if cp.returncode != 0:
            self.log(f"FATAL: 创建容器失败: {name} -> {cp.stderr.strip()}")
            sys.exit(2)
        if container_ip:
            self.log(f"created container: {name} ip={container_ip}")
        else:
            self.log(f"created container: {name}")

    def start_container(self, name: str) -> None:
        """启动容器"""
        cp = self.run_cmd(["docker", "start", name])
        if cp.returncode != 0:
            self.log(f"FATAL: 启动容器失败: {name} -> {cp.stderr.strip()}")
            sys.exit(2)
        self.log(f"started container: {name}")

    def disable_offload_once(self, name: str) -> None:
        """关闭容器的包合并（TSO/GSO/GRO），仅执行一次"""
        shell = r'''
            if [ -f /tmp/.offload_disabled ]; then
                exit 0
            fi
            if command -v sudo >/dev/null 2>&1; then
                sudo ethtool -K eth0 tso off gso off gro off
            else
                ethtool -K eth0 tso off gso off gro off
            fi
            rc=$?
            if [ $rc -eq 0 ]; then
                touch /tmp/.offload_disabled
            fi
            exit $rc
        '''
        cp = self.run_cmd(["docker", "exec", name, "sh", "-lc", shell])
        if cp.returncode == 0:
            self.log(f"{name}: offload disabled (TSO/GSO/GRO off)")
        else:
            msg = (cp.stderr or cp.stdout).strip()
            self.log(f"WARN: {name}: 关闭包合并失败：{msg if msg else 'unknown error'}")

    def remove_containers(self) -> None:
        """删除所有同前缀的容器"""
        subprocess.run(
            f'docker ps -aq -f "name=^{self.CONTAINER_PREFIX}" | xargs -r docker rm -f',
            shell=True, check=False
        )

    def build_container_names(self) -> List[str]:
        """构建容器名列表"""
        return [f"{self.CONTAINER_PREFIX}{i}" for i in range(self.CONTAINER_COUNT)]

    def get_default_action_source(self) -> Path:
        """Return the fallback action.py path."""
        return Path(_project_root) / "traffic_capture_single_csv" / "action.py"

    def ensure_host_code_path_ready(self, ensure_action: bool = True) -> Path:
        """Ensure HOST_CODE_PATH exists and optionally backfill action.py."""
        host_code = Path(self.HOST_CODE_PATH)
        if host_code.exists() and not host_code.is_dir():
            raise NotADirectoryError(f"HOST_CODE_PATH exists but is not a directory: {host_code}")

        if not host_code.exists():
            host_code.mkdir(parents=True, exist_ok=True)
            self.log(f"created HOST_CODE_PATH: {host_code}")

        if ensure_action:
            action_src = self.get_default_action_source()
            if not action_src.exists() or not action_src.is_file():
                raise FileNotFoundError(f"default action.py not found: {action_src}")

            action_dst = host_code / "action.py"
            if action_dst.exists() and not action_dst.is_file():
                raise IsADirectoryError(f"target action.py exists but is not a file: {action_dst}")
            if not action_dst.exists():
                shutil.copy2(action_src, action_dst)
                self.chown_recursive(str(action_dst))
                self.log(f"copied default action.py to: {action_dst}")

        return host_code

    def prepare_pool_once(self) -> List[str]:
        """准备容器池，返回容器名列表"""
        self.ensure_docker_available()

        host_code = self.ensure_host_code_path_ready()
        if not host_code.exists():
            self.log(f"WARN: 宿主机代码目录不存在：{host_code}，仍会尝试挂载。")
        if not host_code.is_absolute():
            self.log(f"WARN: 建议使用绝对路径挂载，当前={host_code}")

        names = self.build_container_names()
        self.log("checking and creating containers...")
        self.log(f"容器池规模={len(names)}: {names[0]} … {names[-1]}")
        container_specs = self.build_container_specs(names)
        if container_specs and container_specs[0][2] is not None:
            self.log(f"固定 IP 范围={container_specs[0][2]} -> {container_specs[-1][2]}")

        created: List[str] = []
        created_lock = threading.Lock()

        def check_and_create(spec: Tuple[int, str, Optional[str]]) -> None:
            _, name, expected_ip = spec
            exists = self.container_exists(name)
            if exists is None:
                self.create_container(name, str(host_code), self.DOCKER_IMAGE, container_ip=expected_ip)
                with created_lock:
                    created.append(name)
                return
            if expected_ip:
                current_ip = self.get_container_ipv4(name)
                if current_ip != expected_ip:
                    self.log(
                        f"WARN: {name} 当前 IP={current_ip or 'unknown'}，"
                        f"期望 IP={expected_ip}，将重建容器"
                    )
                    self.remove_container(name)
                    self.create_container(name, str(host_code), self.DOCKER_IMAGE, container_ip=expected_ip)
                    with created_lock:
                        created.append(name)

        # Pass 1：并发创建不存在的容器
        with ThreadPoolExecutor(max_workers=min(len(names), 20)) as pool:
            pool.map(check_and_create, container_specs)

        # Pass 2：启动未运行的容器
        for n in names:
            if not self.container_running(n):
                self.start_container(n)

        time.sleep(5)

        # Pass 3：对新建容器关闭 offload
        for n in created:
            self.disable_offload_once(n)

        return names

    # ============== 文件操作 ==============
    def clear_host_code_subdirs(self) -> None:
        """清理 HOST_CODE_PATH 下的临时子目录，保留 tools"""
        base_path = self.ensure_host_code_path_ready()
        if not base_path.exists() or not base_path.is_dir():
            self.log(f"WARN: HOST_CODE_PATH 不存在或不是目录：{base_path}")
            return

        for entry in base_path.iterdir():
            if entry.is_dir() and entry.name != 'tools':
                try:
                    shutil.rmtree(entry)
                    self.log(f"删除子目录: {entry}")
                except Exception as e:
                    self.log(f"WARN: 删除子目录失败: {entry} -> {e}")


    def chown_recursive(self, path: str, uid: int = None, gid: int = None) -> None:
        """递归设置文件/目录的所有者"""
        uid = uid or self.DEFAULT_UID
        gid = gid or self.DEFAULT_GID
        try:
            os.chown(path, uid, gid, follow_symlinks=False)
        except Exception:
            pass
        if os.path.isdir(path):
            for root, dirs, files in os.walk(path, followlinks=False):
                for name in dirs:
                    p = os.path.join(root, name)
                    try:
                        os.chown(p, uid, gid, follow_symlinks=False)
                    except Exception:
                        pass
                for name in files:
                    p = os.path.join(root, name)
                    try:
                        os.chown(p, uid, gid, follow_symlinks=False)
                    except Exception:
                        pass

    def move_and_chown(self, src: str, dst_dir: str) -> str:
        """移动文件并设置所有者"""
        os.makedirs(dst_dir, exist_ok=True)
        new_path = shutil.move(src, dst_dir)
        self.chown_recursive(new_path)
        return new_path

    # ============== CSV 操作 ==============
    def read_jobs_from_csv(self, csv_path: str) -> Tuple[List[Dict[str, str]], List[str]]:
        """从 CSV 读取任务"""
        p = Path(csv_path)
        if not p.exists():
            return [], ["id", "url", "domain"]

        with p.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                return [], ["id", "url", "domain"]

            header_fields = [h.strip() for h in reader.fieldnames]

            def get_case_insensitive(row: Dict[str, str], key: str) -> str:
                for k, v in row.items():
                    if isinstance(k, str) and k.lower() == key:
                        return (v or "").strip()
                return ""

            jobs: List[Dict[str, str]] = []
            for r in reader:
                rid = get_case_insensitive(r, "id")
                url = get_case_insensitive(r, "url")
                dom = get_case_insensitive(r, "domain")
                if not url:
                    continue
                jobs.append({"row_id": rid, "url": url, "domain": dom})

        return jobs, header_fields

    def remove_from_csv(self, csv_path: str, row_id: str) -> None:
        """从 CSV 中删除指定记录（原子操作，一次只删除一条）"""
        target_id = str(row_id).strip()
        if not target_id:
            return

        with self._csv_lock:
            p = Path(csv_path)
            if not p.exists():
                return

            with p.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                if reader.fieldnames is None:
                    return
                header_fields = list(reader.fieldnames)
                rows = list(reader)

            def get_id(row: Dict[str, str]) -> str:
                for k, v in row.items():
                    if isinstance(k, str) and k.lower() == "id":
                        return (v or "").strip()
                return ""

            # 只删除第一条匹配的记录
            remaining_rows = []
            removed = False
            for r in rows:
                if not removed and get_id(r) == target_id:
                    removed = True  # 跳过第一条匹配的记录
                else:
                    remaining_rows.append(r)

            if not removed:
                return

            tmp_fd, tmp_path = tempfile.mkstemp(dir=p.parent, suffix=".tmp", prefix=".csv_")
            try:
                with os.fdopen(tmp_fd, "w", encoding="utf-8-sig", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=header_fields, extrasaction="ignore")
                    writer.writeheader()
                    writer.writerows(remaining_rows)
                os.replace(tmp_path, csv_path)
                self.log(f"已从 CSV 删除记录 row_id={row_id}，剩余 {len(remaining_rows)} 条")
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise

    def _wait_before_first_exec(self, container: str) -> None:
        """仅在每个容器第一次执行 docker exec 前做全局节流。"""
        interval = max(float(self.FIRST_EXEC_INTERVAL), 0.0)
        with self._first_exec_lock:
            if container in self._first_exec_done_containers:
                return
            now = time.monotonic()
            scheduled = max(now, self._first_exec_next_ts)
            self._first_exec_next_ts = scheduled + interval
            self._first_exec_done_containers.add(container)

        wait = scheduled - now
        if wait > 0:
            time.sleep(wait)

    # ============== 进度条 ==============
    def _update_progress(self, ok: bool, task_elapsed: float = 0.0) -> None:
        """更新全局进度条"""
        with self._stats_lock:
            if ok:
                self._global_ok += 1
            else:
                self._global_fail += 1

            total_done = self._global_ok + self._global_fail
            elapsed = time.time() - self._global_start_time
            elapsed_min = elapsed / 60.0

            # 计算统计数据
            per_min = total_done / elapsed_min if elapsed_min > 0 else 0
            # 平均耗时按并发容器折算：运行时长 * 容器总数 / 完成任务数
            avg_time = (elapsed * self._global_container_count / total_done) if total_done > 0 else 0

            if self._pbar is not None:
                self._pbar.set_description(
                    f"任务进度: {total_done}个 [运行: {elapsed_min:.1f}分钟 | 成功: {self._global_ok} | 失败: {self._global_fail} | "
                    f"每分钟: {per_min:.2f} | 平均耗时: {avg_time:.1f}秒]"
                )
                self._pbar.update(1)

    # ============== 任务执行 ==============
    def exec_once(self, task: Dict[str, str]) -> Tuple[bool, str]:
        """执行单个任务"""
        container = task["container"]
        self._wait_before_first_exec(container)
        payload = json.dumps(task, ensure_ascii=False)
        cmd = [
            "docker", "exec", container,
            "python", "-u", f"{self.CONTAINER_CODE_PATH}/action.py",
            payload
        ]
        self.log("执行命令", cmd)
        cp = self.run_cmd(cmd, timeout=self.DOCKER_EXEC_TIMEOUT)

        if cp.returncode == 0:
            try:
                return self.process_result(task, container)
            except (json.JSONDecodeError, FileNotFoundError, OSError) as e:
                return False, f"post-processing error: {e}"
        stderr = (cp.stderr or "").strip()
        stdout = (cp.stdout or "").strip()
        if stderr or stdout:
            detail = stderr or stdout
        else:
            detail = "stdout/stderr empty"
        return False, f"docker exec rc={cp.returncode}: {detail}"

    def process_result(self, task: Dict[str, str], container: str) -> Tuple[bool, str]:
        """处理任务执行结果，子类可覆盖"""
        meta_path = os.path.join(self.HOST_CODE_PATH, "meta", f"{container}_last.json")
        with open(meta_path, "r", encoding="utf-8") as f:
            result = json.load(f)

        self.log('result', result)

        pcap_path = result.get("pcap_path")
        ssl_key_file_path = result.get("ssl_key_file_path")
        content_path = result.get("content_path")
        html_path = result.get("html_path")
        screenshot_path = result.get("screenshot_path")
        current_url = result.get("current_url", "")

        if not all([pcap_path, ssl_key_file_path, content_path, html_path, screenshot_path]):
            return False, "result JSON missing required paths"

        # 转换容器路径为宿主机路径
        pcap_path = pcap_path.replace("/app", self.HOST_CODE_PATH)
        ssl_key_file_path = ssl_key_file_path.replace("/app", self.HOST_CODE_PATH)
        content_path = content_path.replace("/app", self.HOST_CODE_PATH)
        html_path = html_path.replace("/app", self.HOST_CODE_PATH)
        screenshot_path = screenshot_path.replace("/app", self.HOST_CODE_PATH)

        # 构建目标目录
        domain = task.get('domain', 'unknown')
        dst = os.path.join(self.BASE_DST, domain)

        # 移动文件
        new_pcap = self.move_and_chown(pcap_path, os.path.join(dst, 'pcap'))
        new_ssl = self.move_and_chown(ssl_key_file_path, os.path.join(dst, 'ssl_key'))
        new_content = self.move_and_chown(content_path, os.path.join(dst, 'content'))
        new_html = self.move_and_chown(html_path, os.path.join(dst, 'html'))
        new_screenshot = self.move_and_chown(screenshot_path, os.path.join(dst, 'screenshot'))

        # 调用成功回调
        self.on_task_success(task, {
            'pcap': new_pcap,
            'ssl_key': new_ssl,
            'content': new_content,
            'html': new_html,
            'screenshot': new_screenshot,
            'current_url': current_url
        })

        return True, ""

    def on_task_success(self, task: Dict[str, str], paths: Dict[str, str]) -> None:
        """任务成功回调，子类可覆盖（如写数据库）"""
        pass

    def on_task_failed(self, task: Dict[str, str], error: str) -> None:
        """任务失败回调，子类可覆盖（如标记数据库）"""
        pass

    def _handle_final_failure(self, task: Dict[str, str], err: str,
                               stats: Dict[str, Any], task_start_time: float) -> None:
        """处理最终失败的任务"""
        row_id = task.get("row_id", "")
        url = task.get("url", "")
        container = task.get("container", "unknown")
        task_elapsed = time.time() - task_start_time

        self.log(f"{container} -> give up [{row_id}] {url} after {self.RETRY + 1} attempts")
        self.on_task_failed(task, err)
        with self._stats_lock:
            stats["fail"] += 1
            stats["errors"].append((task, err))
        self._update_progress(ok=False, task_elapsed=task_elapsed)

    def worker_loop(self, container: str, q: "queue.Queue[Dict[str, str]]",
                    stats: Dict[str, Any], retry: int) -> None:
        """Worker 循环，失败任务放回队列由其他容器重试"""
        while True:
            try:
                task = q.get_nowait()
            except queue.Empty:
                return

            row_id = task.get("row_id", "")
            url = task.get("url", "")
            task["container"] = container

            # 记录重试次数和首次开始时间
            attempt = task.get("_retry_count", 0)
            if "_start_time" not in task:
                task["_start_time"] = time.time()
            task_start_time = task["_start_time"]

            if attempt == 0:
                self.log(f"{container} -> start [{row_id}] {url}")
            else:
                self.log(f"{container} -> retry {attempt}/{retry} [{row_id}] {url}")

            try:
                ok, err = self.exec_once(task)
                if ok:
                    task_elapsed = time.time() - task_start_time
                    self.log(f"{container} -> done  [{row_id}] {url} ({task_elapsed:.1f}s)")
                    with self._stats_lock:
                        stats["ok"] += 1
                    self._update_progress(ok=True, task_elapsed=task_elapsed)
                else:
                    self.log(f"{container} -> fail  [{row_id}] {err[:200]}")
                    # 失败后放回队列，让其他容器重试
                    if attempt < retry:
                        task["_retry_count"] = attempt + 1
                        time.sleep(2)
                        q.put(task)
                    else:
                        self._handle_final_failure(task, err, stats, task_start_time)

            except subprocess.TimeoutExpired:
                err = f"timeout>{self.DOCKER_EXEC_TIMEOUT}s"
                self.log(f"{container} -> timeout [{row_id}] {url}")
                if attempt < retry:
                    task["_retry_count"] = attempt + 1
                    time.sleep(2)
                    q.put(task)
                else:
                    self._handle_final_failure(task, err, stats, task_start_time)

            except Exception as e:
                err = repr(e)
                self.log(f"{container} -> error [{row_id}] {err}")
                if attempt < retry:
                    task["_retry_count"] = attempt + 1
                    time.sleep(2)
                    q.put(task)
                else:
                    self._handle_final_failure(task, err, stats, task_start_time)

            q.task_done()

    # ============== 信号处理 ==============
    def _signal_handler(self, signum, _frame) -> None:
        """信号处理函数"""
        self.log(f"收到中断信号({signum})，立即退出。")
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        finally:
            os._exit(128 + signum)

    # ============== 抽象方法 ==============
    @abstractmethod
    def fetch_jobs(self) -> List[Dict[str, str]]:
        """获取任务列表，子类必须实现"""
        pass

    # ============== 主运行逻辑 ==============
    def run_once(self, names: List[str], jobs: List[Dict[str, str]]) -> Dict[str, Any]:
        """执行一批任务"""
        q: "queue.Queue[Dict[str, str]]" = queue.Queue()
        for t in jobs:
            q.put(t)

        stats: Dict[str, Any] = {"ok": 0, "fail": 0, "errors": []}
        self.log(f"开始执行：jobs={len(jobs)}，并发容器={len(names)}，镜像={self.DOCKER_IMAGE}")

        with ThreadPoolExecutor(max_workers=len(names)) as pool:
            for n in names:
                pool.submit(self.worker_loop, n, q, stats, self.RETRY)
            q.join()

        return stats

    def run(self) -> None:
        """主运行入口"""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # 初始化（子类可覆盖 setup 方法）
        self.setup()

        # 准备容器池
        names: List[str] = []

        self._global_container_count = max(len(names), 1)
        self._global_start_time = time.time()
        self._global_ok = 0
        self._global_fail = 0
        self._runtime_prepared = False
        batch_num = 0

        # 创建常驻进度条（total=None 表示未知总数）
        self._pbar = tqdm(total=None, unit="个", position=0, leave=True,
                         bar_format='{desc}')
        self._pbar.set_description("任务进度: 0个 [初始化中...]")

        try:
            while True:
                jobs = self.fetch_jobs()
                self.log(f"fetched jobs: {len(jobs)}")
                if not jobs:
                    self.log("没有可处理的任务，退出。")
                    break

                if not self._runtime_prepared:
                    self.remove_containers()
                    self.clear_host_code_subdirs()
                    names = self.prepare_pool_once()
                    self._global_container_count = max(len(names), 1)
                    self._runtime_prepared = True

                batch_num += 1
                self.log(f"===== 批次 {batch_num}: 共 {len(jobs)} 条任务 =====")

                stats = self.run_once(names, jobs)

                self.log(f"[批次 {batch_num} 汇总] success={stats['ok']} fail={stats['fail']} total={len(jobs)}")
                if stats["errors"]:
                    self.log("失败样例：")
                    for task, err in stats["errors"][:10]:
                        self.log(f" - id={task.get('row_id','')} url={task.get('url','')} err={err[:200]}")

                if self.CLEAR_HOST_CODE_SUBDIRS_AFTER_BATCH:
                    self.clear_host_code_subdirs()

                # 如果只需要运行一次，跳出循环
                if not self.should_continue():
                    break

        except Exception as e:
            self.log(f"WARN: 执行异常：{e}")
        finally:
            # 关闭进度条
            if self._pbar is not None:
                self._pbar.close()
                self._pbar = None
            if self._runtime_prepared or self.should_cleanup_when_idle():
                self.cleanup()

        # 最终汇总
        elapsed = time.time() - self._global_start_time
        elapsed_min = elapsed / 60.0
        total_done = self._global_ok + self._global_fail
        per_min = total_done / elapsed_min if elapsed_min > 0 else 0
        avg_time = (elapsed * self._global_container_count / total_done) if total_done > 0 else 0
        self.log(f"[最终汇总] 批次={batch_num} | 运行时间={elapsed_min:.1f}分钟 | 总数={total_done} | "
                 f"成功={self._global_ok} | 失败={self._global_fail} | 每分钟={per_min:.2f} | 平均耗时={avg_time:.1f}秒")

    def setup(self) -> None:
        """初始化设置，子类可覆盖"""
        pass

    def cleanup(self) -> None:
        """清理工作，子类可覆盖"""
        pass

    def should_continue(self) -> bool:
        """是否继续下一批次，子类可覆盖"""
        return True

    def should_cleanup_when_idle(self) -> bool:
        """空任务直接退出时，是否仍执行 cleanup。"""
        return type(self).setup is not BaseTrafficIngestor.setup

    @classmethod
    def main(cls) -> None:
        """入口方法"""
        ingestor = cls()
        ingestor.run()
        time.sleep(300)
