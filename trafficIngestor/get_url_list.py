#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
get_url_list.py

批量爬取网站子页面 URL 列表的调度脚本。
- 管理 Docker 容器池
- 并发下发 JSON 任务到 /app/action.py
- /app 挂载 url_list_collector
- 读取 meta/{container}_last.json 处理结果
"""

from __future__ import annotations

import csv
import json
import os
import queue
import shutil
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse
from tqdm import tqdm

# 添加项目根目录到路径
# 当前脚本所在目录的绝对路径（trafficIngestor/）
_current_dir = os.path.dirname(os.path.abspath(__file__))
# 项目根目录的绝对路径（TrafficIngestor/）
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def get_real_username() -> str:
    """获取真实用户名。

    在 sudo 环境下优先读取 SUDO_USER，否则读取 USER 环境变量，
    最后回退到 os.getlogin()。用于生成容器名前缀。

    Returns:
        str: 当前登录用户的用户名。
    """
    return os.environ.get("SUDO_USER") or os.environ.get("USER") or os.getlogin()


# ============== 配置 ==============

# 容器与挂载目录的基础名称
BASE_NAME = "url_list_collector"
# Docker 容器名前缀，格式为 "{用户名}_{BASE_NAME}"
CONTAINER_PREFIX = f"{get_real_username()}_{BASE_NAME}"
# Docker 容器池的容器数量
CONTAINER_COUNT = 400
# 宿主机上 url_list_collector 代码目录的绝对路径，会被挂载到容器内 /app
HOST_CODE_PATH = os.path.join(_project_root, BASE_NAME)
# 爬虫使用的 Docker 镜像名称
DOCKER_IMAGE = "chuanzhoupan/trace_spider:250912"
# 任务失败后的最大重试次数
RETRY = 3
DOCKER_DNS: Optional[str] = None

# 源数据 CSV 文件路径，包含待爬取的网站列表
SOURCE_CSV = os.path.join(_project_root, "small_tools", "top4000.csv")
# 输出结果的根目录，每个域名会在此目录下创建子目录存放 url_list.csv
OUTPUT_BASE_DIR = "/netdisk/ww/top2000/subpages"
# 记录失败任务的 CSV 文件路径
FAILED_CSV = os.path.join(_current_dir, "get_url_list_failed.csv")
# 每个域名需要采集的目标 URL 数量
TARGET_URLS_PER_DOMAIN = 40
# 结束后是否清理容器和临时子目录（默认关闭，便于排查）
CLEANUP_ON_EXIT = False
# 启动前是否清理上一轮遗留的临时文件
STARTUP_CLEANUP_RESIDUAL_FILES = True
# ==================================

# 输出 CSV 文件的列头：序号、URL、域名
OUTPUT_HEADER = ["id", "url", "domain"]
# 失败记录 CSV 文件的列头：种子ID、种子URL、域名、已采集数量、错误信息
FAILED_HEADER = ["seed_id", "seed_url", "domain", "collected_count", "error"]

# 保护统计计数器（stats dict）的线程锁
_stats_lock = threading.Lock()

# 保护输出文件写入和 _domain_url_set 更新的线程锁
_output_lock = threading.Lock()
# 保护失败记录 CSV 写入的线程锁
_failed_lock = threading.Lock()
# 保护进度条对象的线程锁
_pbar_lock = threading.Lock()
# 全局域名→已采集URL集合的映射，用于去重和断点续传
_domain_url_set: Dict[str, Set[str]] = {}
_pbar: Optional[tqdm] = None


def log(*a, container: Optional[str] = None) -> None:
    """带时间戳的日志输出。"""
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    msg = f"[{ts}] " + " ".join(str(x) for x in a)
    with _pbar_lock:
        if _pbar is not None:
            tqdm.write(msg)
        else:
            print(msg, flush=True)


def _update_progress(stats: Dict[str, Any]) -> None:
    """更新全局进度条描述和进度。"""
    with _pbar_lock:
        if _pbar is None:
            return
        total_done = int(stats.get("ok", 0)) + int(stats.get("fail", 0))
        start_time = float(stats.get("start_time", time.time()))
        container_count = int(stats.get("container_count", 1)) or 1
        elapsed = time.time() - start_time
        elapsed_min = elapsed / 60.0 if elapsed > 0 else 0.0
        per_min = (total_done / elapsed_min) if elapsed_min > 0 else 0.0
        avg_time = (elapsed * container_count / total_done) if total_done > 0 else 0.0
        _pbar.set_description(
            f"任务进度: {total_done}个 [运行: {elapsed_min:.1f}分钟 | 成功: {stats['ok']} | "
            f"失败: {stats['fail']} | 每分钟: {per_min:.2f} | 平均耗时: {avg_time:.1f}秒]"
        )
        _pbar.update(1)


def run(cmd: List[str], timeout: Optional[int] = None) -> subprocess.CompletedProcess:
    """执行外部命令并捕获输出。"""
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout)


def ensure_docker_available() -> None:
    """检查 Docker 是否可用，不可用则退出。"""
    try:
        run(["docker", "version"]).check_returncode()
    except Exception as e:
        log("FATAL: docker 不可用。", e)
        sys.exit(2)


def container_exists(name: str) -> Optional[bool]:
    """检查容器是否存在。返回 True/False 表示存在，None 表示不存在。"""
    cp = run(["docker", "inspect", "-f", "{{.State.Running}}", name])
    if cp.returncode != 0:
        return None
    out = cp.stdout.strip().lower()
    return (out == "true") or (out == "false")


def container_running(name: str) -> bool:
    """检查容器是否正在运行。"""
    cp = run(["docker", "inspect", "-f", "{{.State.Running}}", name])
    return (cp.returncode == 0) and (cp.stdout.strip().lower() == "true")


def _safe_uid_gid() -> Tuple[str, str]:
    """安全地获取当前用户的 UID 和 GID。"""
    uid = os.environ.get("SUDO_UID")
    gid = os.environ.get("SUDO_GID")
    if uid and gid:
        return uid, gid
    try:
        return str(os.getuid()), str(os.getgid())
    except Exception:
        return "1000", "1000"


def _docker_dns_args() -> List[str]:
    """Return optional explicit Docker DNS args when overriding daemon defaults."""
    dns_server = (DOCKER_DNS or "").strip()
    return ["--dns", dns_server] if dns_server else []


def create_container(name: str, host_code_path: str, image: str) -> None:
    """创建新的 Docker 容器，挂载代码目录和 tools 目录。"""
    uid, gid = _safe_uid_gid()
    tools_path = os.path.join(_project_root, "tools")
    cmd = [
        "docker", "run",
        "--init",
        "--dns", "172.17.0.1",
        "--volume", f"{host_code_path}:/app",
        "-e", f"HOST_UID={uid}",
        "-e", f"HOST_GID={gid}",
        "--privileged",
        "-itd",
        "--name", name, image, "/bin/bash",
    ]
    dns_args = _docker_dns_args()
    if dns_args:
        cmd[3:5] = dns_args
    else:
        del cmd[3:5]
    cp = run(cmd)
    if cp.returncode != 0:
        log(f"FATAL: 创建容器失败: {name} -> {cp.stderr.strip()}", container=name)
        sys.exit(2)
    log(f"created container: {name}", container=name)


def start_container(name: str) -> None:
    """启动一个已停止的 Docker 容器。"""
    cp = run(["docker", "start", name])
    if cp.returncode != 0:
        log(f"FATAL: 启动容器失败: {name} -> {cp.stderr.strip()}", container=name)
        sys.exit(2)
    log(f"started container: {name}", container=name)


def disable_offload_once(name: str) -> None:
    """在容器内关闭网卡 TSO/GSO/GRO（仅执行一次），提高流量采集精度。"""
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
    cp = run(["docker", "exec", name, "sh", "-lc", shell])
    if cp.returncode == 0:
        log(f"{name}: offload disabled (TSO/GSO/GRO off)", container=name)
    else:
        msg = (cp.stderr or cp.stdout).strip()
        log(f"WARN: {name}: 关闭包合并失败：{msg if msg else 'unknown error'}", container=name)


def build_container_names(prefix: str, count: int) -> List[str]:
    """根据前缀和数量生成容器名称列表。"""
    return [f"{prefix}{i}" for i in range(count)]


def prepare_pool_once() -> List[str]:
    """一次性初始化 Docker 容器池：并发创建/启动容器，关闭网卡 offload。"""
    ensure_docker_available()

    host_code = Path(HOST_CODE_PATH)
    if not host_code.exists():
        log(f"WARN: 宿主机代码目录不存在：{host_code}，仍会尝试挂载。")
    if not host_code.is_absolute():
        log(f"WARN: 建议使用绝对路径挂载，当前={host_code}")

    names = build_container_names(CONTAINER_PREFIX, CONTAINER_COUNT)
    log(f"容器池规模={len(names)}: {names[0]} … {names[-1]}")

    created: List[str] = []
    created_lock = threading.Lock()

    def check_and_create(name: str) -> None:
        exists = container_exists(name)
        if exists is None:
            create_container(name, str(host_code), DOCKER_IMAGE)
            with created_lock:
                created.append(name)

    with ThreadPoolExecutor(max_workers=min(len(names), 20)) as pool:
        pool.map(check_and_create, names)

    for n in names:
        if not container_running(n):
            start_container(n)

    time.sleep(5)
    for n in created:
        disable_offload_once(n)

    return names


def remove_containers() -> None:
    """删除所有以 CONTAINER_PREFIX 为前缀的 Docker 容器。"""
    subprocess.run(
        f'docker ps -aq -f "name=^{CONTAINER_PREFIX}" | xargs -r docker rm -f',
        shell=True,
        check=False
    )


def cleanup_startup_residual_files(container_names: List[str]) -> None:
    """清理启动前遗留的临时文件，避免旧结果污染新一轮任务。"""
    base = Path(HOST_CODE_PATH)
    if not base.exists() or not base.is_dir():
        log(f"WARN: 启动前清理跳过，代码目录不存在：{base}")
        return

    removed = 0

    def _clear_dir_contents(dir_path: Path) -> None:
        nonlocal removed
        if not dir_path.exists() or not dir_path.is_dir():
            return
        for entry in dir_path.iterdir():
            try:
                if entry.is_dir():
                    shutil.rmtree(entry)
                else:
                    entry.unlink()
                removed += 1
            except Exception as e:
                log(f"WARN: 删除残余文件失败: {entry} -> {e}")

    # 清理上一轮容器结果文件：meta/{container}_last.json
    meta_dir = base / "meta"
    for name in container_names:
        p = meta_dir / f"{name}_last.json"
        if not p.exists():
            continue
        try:
            p.unlink()
            removed += 1
        except Exception as e:
            log(f"WARN: 删除残余文件失败: {p} -> {e}")

    # 清理 URL 请求轨迹临时文件
    for pattern in ("request_url_*.txt",):
        for p in base.glob(pattern):
            if not p.is_file():
                continue
            try:
                p.unlink()
                removed += 1
            except Exception as e:
                log(f"WARN: 删除残余文件失败: {p} -> {e}")

    # 清理运行时临时目录内容（保留目录）
    for dirname in ("logs", "download", "tools"):
        _clear_dir_contents(base / dirname)

    log(f"启动前清理残余文件完成：removed={removed}")




def normalize_domain(value: str) -> str:
    """将输入标准化为纯域名（补协议前缀 → urlparse → 提取 hostname → 小写）。"""
    text = (value or "").strip()
    if not text:
        return ""
    if "://" not in text:
        text = "https://" + text
    parsed = urlparse(text)
    return (parsed.hostname or "").strip().lower().strip(".")


def normalize_seed_url(value: str) -> str:
    """将输入标准化为完整种子 URL（已有合法 URL 直接返回，否则拼接 https://）。"""
    text = (value or "").strip()
    if not text:
        return ""
    if "://" in text:
        parsed = urlparse(text)
        if parsed.scheme.lower() in ("http", "https") and parsed.netloc:
            return text
    domain = normalize_domain(text)
    if not domain:
        return ""
    return f"https://{domain}"


def domain_output_csv(domain: str) -> str:
    """根据域名生成输出 CSV 路径：{OUTPUT_BASE_DIR}/{domain}/url_list.csv。"""
    return os.path.join(OUTPUT_BASE_DIR, domain, "url_list.csv")


def append_rows(csv_path: str, header: List[str], rows: List[Dict[str, str]]) -> None:
    """追加写入 CSV，文件不存在或为空时自动写入表头。"""
    if not rows:
        return
    p = Path(csv_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    need_header = (not p.exists()) or p.stat().st_size == 0
    with p.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        if need_header:
            writer.writeheader()
        writer.writerows(rows)


def read_source_jobs() -> List[Dict[str, Any]]:
    """从源 CSV 读取任务列表，支持单列/双列格式，自动跳过表头和去重。"""
    p = Path(SOURCE_CSV)
    if not p.exists():
        log(f"WARN: 源 CSV 不存在: {SOURCE_CSV}")
        return []

    jobs: List[Dict[str, Any]] = []
    seen_domains: Set[str] = set()

    with p.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        for line_no, row in enumerate(reader, start=1):
            if not row:
                continue

            first = (row[0] or "").strip()
            second = (row[1] or "").strip() if len(row) > 1 else ""

            # 跳过表头行
            if line_no == 1 and first.lower() in ("id", "rank", "index"):
                continue

            raw_site = second if second else first
            domain = normalize_domain(raw_site)
            if not domain or domain in seen_domains:
                continue
            seen_domains.add(domain)

            row_id = first if first and first.lower() not in ("id", "rank", "index") else str(len(jobs) + 1)
            seed_url = normalize_seed_url(raw_site)
            if not seed_url:
                continue

            jobs.append(
                {
                    "row_id": row_id,
                    "url": seed_url,
                    "domain": domain,
                }
            )

    return jobs


def load_existing_output() -> None:
    """加载历史已采集的 URL 到内存 _domain_url_set，实现断点续传。"""
    jobs = read_source_jobs()
    if not jobs:
        return

    for job in jobs:
        domain = normalize_domain(job.get("domain", ""))
        if not domain:
            continue

        p = Path(domain_output_csv(domain))
        if not p.exists():
            continue

        try:
            with p.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                if reader.fieldnames is None:
                    continue
                for row in reader:
                    url = (row.get("url", "") or "").strip()
                    if not url:
                        continue
                    row_domain = normalize_domain(row.get("domain", "")) or domain
                    if row_domain != domain:
                        continue
                    _domain_url_set.setdefault(domain, set()).add(url)
        except Exception as e:
            log(f"WARN: 读取历史输出失败 {p}: {e}")

    log(
        f"已加载历史输出: domains={len(_domain_url_set)}, "
        f"urls={sum(len(v) for v in _domain_url_set.values())}"
    )


def fetch_jobs() -> List[Dict[str, Any]]:
    """读取源任务并过滤掉已达标域名，返回待处理任务列表。"""
    source_jobs = read_source_jobs()
    pending: List[Dict[str, Any]] = []
    for job in source_jobs:
        domain = job.get("domain", "")
        exist_count = len(_domain_url_set.get(domain, set()))
        if exist_count >= TARGET_URLS_PER_DOMAIN:
            continue
        pending.append(job)

    log(
        f"源站点={len(source_jobs)}, 待处理={len(pending)}, "
        f"目标每站点={TARGET_URLS_PER_DOMAIN} URL"
    )
    return pending


def process_result(task: Dict[str, Any], container: str) -> Tuple[bool, str]:
    """处理爬虫结果：读取 meta JSON → 提取 URL → 去重 → 写入 CSV → 判断是否达标。"""
    meta_path = os.path.join(HOST_CODE_PATH, "meta", f"{container}_last.json")
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            result = json.load(f)
    except Exception as e:
        return False, f"meta read error: {e}"

    domain = normalize_domain(task.get("domain", "")) or normalize_domain(result.get("domain", ""))
    if not domain:
        return False, "invalid domain in task/result"

    raw_urls = result.get("collected_urls", [])
    if not isinstance(raw_urls, list):
        raw_urls = []
    visited_count = int(result.get("visited_count", 0) or 0)

    # 对原始 URL 列表去重（保持顺序）
    unique_urls: List[str] = []
    seen = set()
    for item in raw_urls:
        if not isinstance(item, str):
            continue
        u = item.strip()
        if not u or u in seen:
            continue
        seen.add(u)
        unique_urls.append(u)

    # 线程安全地更新全局去重集合和输出文件
    with _output_lock:
        existing_set = _domain_url_set.setdefault(domain, set())
        new_rows = []

        for u in unique_urls:
            if len(existing_set) >= TARGET_URLS_PER_DOMAIN:
                break
            if u in existing_set:
                continue
            existing_set.add(u)
            next_id = len(existing_set)
            new_rows.append({"id": str(next_id), "url": u, "domain": domain})

        if new_rows:
            append_rows(domain_output_csv(domain), OUTPUT_HEADER, new_rows)

        total = len(existing_set)

    log(f"{domain}: 新增 {len(new_rows)} 条，累计 {total}/{TARGET_URLS_PER_DOMAIN}", container=container)

    if total >= TARGET_URLS_PER_DOMAIN:
        return True, ""

    err = result.get("error", "")
    if err:
        return False, f"insufficient urls: {total}/{TARGET_URLS_PER_DOMAIN}; visited={visited_count}; error={err}"
    return False, f"insufficient urls: {total}/{TARGET_URLS_PER_DOMAIN}; visited={visited_count}"


def on_task_failed(task: Dict[str, Any], error: str) -> None:
    """重试耗尽后，将失败信息写入 FAILED_CSV。"""
    domain = normalize_domain(task.get("domain", ""))
    with _failed_lock:
        rows = [
            {
                "seed_id": task.get("row_id", ""),
                "seed_url": task.get("url", ""),
                "domain": domain,
                "collected_count": str(len(_domain_url_set.get(domain, set()))),
                "error": (error or "")[:500],
            }
        ]
        append_rows(FAILED_CSV, FAILED_HEADER, rows)


def exec_once(task: Dict[str, Any]) -> Tuple[bool, str]:
    """对单个任务执行一次 docker exec，成功则调用 process_result 解析结果。"""
    container = task["container"]
    payload_obj = {
        "row_id": task.get("row_id", ""),
        "url": task.get("url", ""),
        "domain": task.get("domain", ""),
        "container": container,
        "target_urls_per_domain": TARGET_URLS_PER_DOMAIN,
    }
    payload = json.dumps(payload_obj, ensure_ascii=False)
    cmd = [
        "docker", "exec", container,
        "python", "-u", "/app/action.py",
        payload,
    ]
    log("执行命令", cmd, container=container)
    cp = run(cmd, timeout=6000)
    if cp.returncode == 0:
        return process_result(task, container)
    return False, (cp.stderr.strip() or cp.stdout.strip())


def worker_loop(container: str, q: "queue.Queue[Dict[str, Any]]", stats: Dict[str, Any], retry: int) -> None:
    """单个容器的工作循环：取任务 → 执行 → 成功计数 / 失败重试。"""
    while True:
        try:
            task = q.get_nowait()
        except queue.Empty:
            return

        row_id = task.get("row_id", "")
        url = task.get("url", "")
        task["container"] = container

        attempt = int(task.get("_retry_count", 0) or 0)
        if "_start_time" not in task:
            task["_start_time"] = time.time()
        task_start_time = float(task["_start_time"])

        try:
            if attempt == 0:
                log(f"{container} -> start [{row_id}] {url}", container=container)
            else:
                log(f"{container} -> retry {attempt}/{retry} [{row_id}] {url}", container=container)

            ok, err = exec_once(task)
            if ok:
                elapsed = time.time() - task_start_time
                log(f"{container} -> done  [{row_id}] {url} ({elapsed:.1f}s)", container=container)
                with _stats_lock:
                    stats["ok"] += 1
                    _update_progress(stats)
            else:
                log(f"{container} -> fail  [{row_id}] {err[:200]}", container=container)
                if attempt < retry:
                    task["_retry_count"] = attempt + 1
                    time.sleep(2)
                    q.put(task)
                else:
                    with _stats_lock:
                        stats["fail"] += 1
                        stats["errors"].append((task, err))
                        _update_progress(stats)
                    on_task_failed(task, err)

        except subprocess.TimeoutExpired:
            err = f"timeout>6000s"
            log(f"{container} -> timeout [{row_id}] {url}", container=container)
            if attempt < retry:
                task["_retry_count"] = attempt + 1
                time.sleep(2)
                q.put(task)
            else:
                with _stats_lock:
                    stats["fail"] += 1
                    stats["errors"].append((task, err))
                    _update_progress(stats)
                on_task_failed(task, err)

        except Exception as e:
            err = repr(e)
            log(f"{container} -> error [{row_id}] {err}", container=container)
            if attempt < retry:
                task["_retry_count"] = attempt + 1
                time.sleep(2)
                q.put(task)
            else:
                with _stats_lock:
                    stats["fail"] += 1
                    stats["errors"].append((task, err))
                    _update_progress(stats)
                on_task_failed(task, err)

        finally:
            q.task_done()


def run_once(names: List[str], jobs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """执行一轮批量调度：任务入队 → 每容器一线程并发消费 → 返回统计。"""
    global _pbar
    q: "queue.Queue[Dict[str, Any]]" = queue.Queue()
    for task in jobs:
        q.put(task)

    stats: Dict[str, Any] = {
        "ok": 0,
        "fail": 0,
        "errors": [],
        "start_time": time.time(),
        "container_count": max(len(names), 1),
    }
    log(f"开始执行：jobs={len(jobs)}，并发容器={len(names)}，镜像={DOCKER_IMAGE}")

    with tqdm(total=len(jobs), desc="任务进度", leave=True) as pbar:
        with _pbar_lock:
            _pbar = pbar
        try:
            with ThreadPoolExecutor(max_workers=len(names)) as pool:
                futures = []
                for n in names:
                    futures.append(pool.submit(worker_loop, n, q, stats, RETRY))
                q.join()
                for fut in futures:
                    fut.result()
        finally:
            with _pbar_lock:
                _pbar = None

    return stats


def cleanup_host_code_dir() -> None:
    """清理 url_list_collector 下的临时目录，保留 tools、trace_spider、utils。"""
    base = Path(HOST_CODE_PATH)
    if not base.exists() or not base.is_dir():
        return
    for entry in base.iterdir():
        if entry.is_dir() and entry.name not in {"tools", "trace_spider", "utils"}:
            try:
                shutil.rmtree(entry)
                log(f"删除子目录: {entry}")
            except Exception as e:
                log(f"WARN: 删除子目录失败: {entry} -> {e}")


def sig(signum, _frame) -> None:
    """信号处理：收到 SIGINT/SIGTERM 后刷新缓冲区并立即退出。"""
    log(f"收到中断信号({signum})，立即退出。")
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    finally:
        os._exit(128 + signum)


def main() -> None:
    """主入口：初始化容器池 → 加载历史 → 过滤任务 → 并发执行 → 清理。"""
    signal.signal(signal.SIGINT, sig)
    signal.signal(signal.SIGTERM, sig)

    names = prepare_pool_once()
    if STARTUP_CLEANUP_RESIDUAL_FILES:
        cleanup_startup_residual_files(names)
    load_existing_output()
    jobs = fetch_jobs()
    if not jobs:
        log("没有可处理的任务，退出。")
        return

    stats = run_once(names, jobs)
    log(f"[summary] success={stats['ok']} fail={stats['fail']} total={len(jobs)}")
    if stats["errors"]:
        log("失败样例：")
        for task, err in stats["errors"][:10]:
            log(f" - id={task.get('row_id', '')} url={task.get('url', '')} err={str(err)[:200]}")

    if CLEANUP_ON_EXIT:
        cleanup_host_code_dir()
        remove_containers()
    else:
        log("按配置跳过退出清理：保留 docker 容器和 url_list_collector 子目录。")


if __name__ == "__main__":
    remove_containers()
    main()
