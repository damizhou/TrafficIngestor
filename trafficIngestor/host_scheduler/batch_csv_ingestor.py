"""在现有单 CSV 调度器上按域名组合任务并保存共享产物。

每个分组把同一域名的连续 N 行（默认 10 行）压成一个 docker exec 任务，组内页面
共享一份 pcap 和一份 keylog，写入 ``data/<domain>/manifest/*.json`` 后整组成功；
任意页面失败都按整组失败处理，CSV 行全部保留，等待下一轮按组重试。
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from host_scheduler.base_traffic_ingestor import FatalIngestorError
from host_scheduler.csv_ingestor_common import CsvTaskSourceMixin


class BatchCsvTaskSourceMixin(CsvTaskSourceMixin):
    """同域名按固定页数分组，重复记录保留为独立采集组。"""

    BATCH_GROUP_SIZE = 10
    BATCH_PAGE_WAIT_SECONDS = 120
    BATCH_PAGE_GAP_SECONDS = 15

    def fetch_jobs(self):
        if not self._get_csv_path().is_file():
            raise FileNotFoundError(f"CSV 文件不存在: {self.CSV_PATH}")
        groups: dict[str, list[dict[str, str]]] = {}
        for row in super().fetch_jobs():
            domain = self._get_csv_row_value(row, "domain")
            if not domain or domain in {".", ".."} or "/" in domain or "\\" in domain:
                raise FatalIngestorError(f"CSV domain 不是有效目录标签: {domain!r}")
            row["domain"] = domain
            groups.setdefault(domain, []).append(row)

        jobs = []
        for domain, rows in groups.items():
            if len(rows) % self.BATCH_GROUP_SIZE:
                raise FatalIngestorError(
                    f"{domain} 有 {len(rows)} 行，无法按 {self.BATCH_GROUP_SIZE} 页完整分组"
                )
            # 按每个网站在 CSV 中的出现顺序分组，保留重复轮次。
            for offset in range(0, len(rows), self.BATCH_GROUP_SIZE):
                group_rows = rows[offset:offset + self.BATCH_GROUP_SIZE]
                group_index = offset // self.BATCH_GROUP_SIZE + 1
                if len({row["url"] for row in group_rows}) != self.BATCH_GROUP_SIZE:
                    raise FatalIngestorError(
                        f"{domain} 第 {group_index} 组存在重复页面"
                    )
                group_id = f"batch_{group_index:04d}"
                jobs.append({
                    "row_id": group_id,
                    "group_id": group_id,
                    "url": group_rows[0]["url"],
                    "domain": domain,
                    "urls": group_rows,
                })
        return jobs

    def get_default_action_source(self):
        return Path(self.SOURCE_ROOT) / "tools" / "action_batch.py"

    def build_batch_payload(self, task) -> dict:
        """构造容器侧任务 JSON；页面列表过长时改由文件传递。"""
        container = task["container"]
        payload = {
            "container": container,
            "domain": task["domain"],
            "group_id": task["group_id"],
            "container_attempt": task.get("container_attempt", 1),
            "container_history": task.get("container_history", [task["container"]]),
            "browser_label": task.get("browser_label", ""),
            "page_wait_seconds": self.BATCH_PAGE_WAIT_SECONDS,
            "page_gap_seconds": self.BATCH_PAGE_GAP_SECONDS,
            "urls": [
                {"row_id": row["row_id"], "url": row["url"]}
                for row in task["urls"]
            ],
        }
        return payload

    def exec_once(self, task):
        container = task["container"]
        self._wait_before_first_exec(container)
        if self._browser_label:
            task["browser_label"] = self._browser_label
        payload = self.build_batch_payload(task)
        # 用挂载文件传输长列表，避免操作系统的单参数长度限制。
        filename = f"{container}_task.json"
        task_path = Path(self.HOST_CODE_PATH, "meta", filename)
        task_path.parent.mkdir(parents=True, exist_ok=True)
        task_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        cmd = [
            "docker", "exec", container, "python", "-u",
            f"{self.CONTAINER_CODE_PATH}/action.py", "--payload-file",
            f"{self.CONTAINER_CODE_PATH}/meta/{filename}",
        ]
        self.log(
            f"执行分组任务: {task['domain']}/{task['group_id']}，"
            f"{len(task['urls'])} 个页面"
        )
        cp = self.run_cmd(cmd, timeout=self.DOCKER_EXEC_TIMEOUT)
        if cp.returncode != 0:
            return False, f"docker exec rc={cp.returncode}: {cp.stderr or cp.stdout}"
        try:
            return self.process_result(task, container)
        except (json.JSONDecodeError, OSError, ValueError) as exc:
            return False, f"batch post-processing error: {exc}"

    @staticmethod
    def _describe_group_failure(result, visited, expected) -> str:
        """把组内失败页面整理成一行可检索的错误信息。"""
        failures = [row for row in visited if row.get("status") != "ok"]
        if failures:
            first = failures[0]
            parts = [
                f"batch_visit_failed={len(failures)}/{len(expected)}",
                f"row_id={first.get('row_id', '')}",
                f"url={first.get('url', '')}",
                f"reason={first.get('error') or 'unknown'}",
                f"visited={len(visited)}",
            ]
            status = int(first.get("http_status") or 0)
            if status:
                parts.append(f"HTTP_STATUS={status}")
            if status in (404, 410):
                parts.append("PAGE_NOT_FOUND")
            return " ".join(parts)

        error = str(result.get("error") or "").strip()
        if error:
            return f"batch_group_failed: {error}"
        return f"batch result status={result.get('status')!r}"

    def process_result(self, task, container):
        meta_path = Path(self.HOST_CODE_PATH, "meta", f"{container}_last.json")
        result = json.loads(meta_path.read_text(encoding="utf-8"))
        expected = [(row["row_id"], row["url"]) for row in task["urls"]]
        visited = result.get("visited_urls", [])
        actual = [(row.get("row_id"), row.get("url")) for row in visited]
        if (result.get("domain") != task["domain"]
                or result.get("group_id") != task["group_id"]):
            return False, (
                "batch result does not match requested group: "
                f"result={result.get('domain')}/{result.get('group_id')} "
                f"expected={task['domain']}/{task['group_id']}"
            )
        if result.get("url_count") != len(expected):
            return False, (
                f"batch result url_count={result.get('url_count')} "
                f"expected={len(expected)}"
            )
        if result.get("status") != "ok" or actual != expected:
            return False, self._describe_group_failure(result, visited, expected)

        fields = {"pcap": "pcap_path", "ssl_key": "ssl_key_file_path"}
        container_paths = [result.get(field, "") for field in fields.values()]
        if not all(container_paths):
            return False, "batch result missing pcap_path or ssl_key_file_path"
        self.chown_container_result_paths(container, container_paths)
        sources = {}
        workspace = Path(self.HOST_CODE_PATH).resolve()
        for kind, field in fields.items():
            source = Path(self.container_path_to_host_path(result[field])).resolve()
            if not source.is_relative_to(workspace):
                raise ValueError(f"产物路径超出工作目录: {source}")
            if not source.is_file() or source.stat().st_size == 0:
                return False, f"batch artifact missing or empty: {source}"
            sources[kind] = str(source)

        # 访问清单与共享产物一起归档，用于验证每条输入（含重复行）的覆盖情况。
        manifest = meta_path.with_name(f"{Path(sources['pcap']).stem}.json")
        manifest.write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        sources["manifest"] = str(manifest)
        dst = os.path.join(self.get_result_domain_base_dir(), task["domain"])
        move_plan = [
            (source, os.path.join(dst, kind)) for kind, source in sources.items()
        ]
        self.preflight_result_moves(move_plan)
        self.prepare_success_output_tree(dst)
        saved = {
            kind: self.move_and_chown(source, os.path.join(dst, kind), result_root=dst)
            for kind, source in sources.items()
        }
        self._task_success_context.paths = saved
        try:
            self.on_task_success(task, saved)
        finally:
            self._task_success_context.paths = {}
        return True, ""

    def on_task_success(self, task, paths):
        # 只有共享产物全部归档后，才一次性登记删除组内整组行。
        removed = sum(1 for row in task["urls"] if self._mark_csv_row_for_removal(row))
        if removed != len(task["urls"]):
            self.log(
                f"WARN: CSV 仅登记删除 {removed}/{len(task['urls'])} 行: "
                f"{task['domain']}/{task['group_id']}"
            )
        self.log(
            f"采集组已保存: {task['domain']}/{task['group_id']}"
            f"（{len(task['urls'])} 页，pcap={paths.get('pcap', '')}）"
        )

    def on_task_failed(self, task, error):
        # 一份共享抓包无法按 URL 拆分提交，任意失败均保留整组 CSV 行。
        self.log(
            f"分组任务失败，保留全部 CSV 行: {task['domain']}/{task['group_id']}: "
            f"{self.compact_error(error)}"
        )

    def verify_task_completeness(self, csv_path=None):
        manifest_path = csv_path or self._copied_task_csv_path
        if not manifest_path:
            return None
        _, rows = self._read_csv_records(Path(manifest_path))
        domains: dict[str, list[tuple[str, str]]] = {}
        for row in rows:
            url = self._get_csv_row_value(row, "url")
            if not url:
                continue
            domain = self._get_csv_row_value(row, "domain")
            domains.setdefault(domain, []).append(
                (self._get_csv_row_value(row, "id"), url)
            )
        expected = {
            (domain, f"batch_{offset // self.BATCH_GROUP_SIZE + 1:04d}"):
                pages[offset:offset + self.BATCH_GROUP_SIZE]
            for domain, pages in domains.items()
            for offset in range(0, len(pages), self.BATCH_GROUP_SIZE)
        }
        complete = set()
        root = Path(self.get_result_domain_base_dir())
        for domain in domains:
            domain_root = root / domain
            if not domain_root.is_dir():
                continue
            for path in (domain_root / "manifest").glob("*.json"):
                try:
                    result = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    continue
                key = (domain, result.get("group_id"))
                visited = result.get("visited_urls", [])
                if (result.get("status") != "ok"
                        or result.get("domain") != domain
                        or any(row.get("status") != "ok" for row in visited)
                        or [(row.get("row_id"), row.get("url")) for row in visited]
                        != expected.get(key)):
                    continue
                artifacts = [
                    domain_root / kind / Path(result.get(field, "")).name
                    for kind, field in (
                        ("pcap", "pcap_path"),
                        ("ssl_key", "ssl_key_file_path"),
                    )
                ]
                if not all(path.is_file() and path.stat().st_size > 0
                           for path in artifacts):
                    continue
                complete.add(key)
        report = {
            "expected_groups": len(expected),
            "matched_groups": len(complete),
            "missing_groups": len(expected) - len(complete),
            "expected_pages": sum(map(len, expected.values())),
            "matched_pages": sum(len(expected[key]) for key in complete),
        }
        self.log(f"分组任务完整度校验: {report}")
        return report
