"""离线分组采集回归：python tests/test_batch_csv_ingestor.py。"""

import csv
import importlib.util
import json
import queue
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "trafficIngestor"))

from host_scheduler.base_traffic_ingestor import FatalIngestorError
from host_scheduler.csv_ingestor_common import CsvIngestorProfile, CsvTaskSourceMixin
from host_scheduler.single_csv_profiles import (
    build_ingestor, load_profile_definition, run_profile, BATCH_RUN_POLICY,
)


class BatchSchedulerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.csv = self.root / "input.csv"
        rows = []
        for _ in range(2):
            for domain in ("a.test", "b.test"):
                rows.extend((str(i), f"https://{domain}/{i}", domain) for i in range(1, 11))
        with self.csv.open("w", encoding="utf-8", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["id", "url", "domain"])
            writer.writerows(rows)
        definition = load_profile_definition(ROOT / "trafficIngestor/single_csv/batch.py")
        config = dict(definition.profile.class_attributes)
        config.update(
            CSV_PATH=str(self.csv), BASE_DST=str(self.root / "output"),
            HOST_CODE_PATH=str(self.root / "workspace"),
            NORMALIZE_SUCCESS_OUTPUT_MODES=False,
        )
        from dataclasses import replace
        self.definition = replace(definition, profile=CsvIngestorProfile(config))
        self.cls = build_ingestor(self.definition, __name__)
        self.ingestor = self.cls()
        self.ingestor.log = Mock()
        self.ingestor.chown_container_result_paths = Mock()
        self.ingestor.chown_recursive = Mock()
        self.ingestor.chown_path = Mock()
        self.ingestor._mark_csv_row_for_removal = Mock()
        self.jobs = self.ingestor.fetch_jobs()

    def make_result(self, task, failed=None):
        workspace = Path(self.ingestor.HOST_CODE_PATH)
        (workspace / "meta").mkdir(parents=True, exist_ok=True)
        (workspace / "data").mkdir(exist_ok=True)
        (workspace / "ssl_key").mkdir(exist_ok=True)
        pcap = workspace / "data/group.pcap"
        pcap.write_bytes(
            struct.pack("<IHHIIII", 0xa1b2c3d4, 2, 4, 0, 0, 65535, 1)
            + struct.pack("<IIII", 1, 0, 4, 4) + b"test"
        )
        (workspace / "ssl_key/group_ssl_key.log").write_text("keylog", encoding="utf-8")
        result = {
            "domain": task["domain"], "group_id": task["group_id"],
            "status": "ok" if failed is None else "fail",
            "pcap_path": "/app/data/group.pcap",
            "ssl_key_file_path": "/app/ssl_key/group_ssl_key.log",
            "url_count": len(task["urls"]),
            "visited_urls": [
                {"row_id": row["row_id"], "url": row["url"],
                 "status": "fail" if index == failed else "ok"}
                for index, row in enumerate(task["urls"])
            ],
        }
        (workspace / "meta/test_last.json").write_text(json.dumps(result), encoding="utf-8")
        return result

    def test_groups_keep_ten_pages_and_repeated_rounds(self):
        self.assertEqual(len(self.jobs), 4)
        self.assertEqual([j["group_id"] for j in self.jobs],
                         ["batch_0001", "batch_0002", "batch_0001", "batch_0002"])
        for job in self.jobs:
            self.assertEqual([r["row_id"] for r in job["urls"]], list(map(str, range(1, 11))))
            self.assertEqual({r["domain"] for r in job["urls"]}, {job["domain"]})
        self.assertEqual(self.ingestor.fetch_jobs(), [])

    def test_success_archives_one_shared_pair_and_manifest_without_editing_csv(self):
        before = self.csv.read_bytes()
        self.make_result(self.jobs[0])
        self.assertEqual(self.ingestor.process_result(self.jobs[0], "test"), (True, ""))
        output = Path(self.ingestor.get_result_domain_base_dir()) / "a.test"
        self.assertEqual(len(list((output / "pcap").glob("*.pcap"))), 1)
        self.assertEqual(len(list((output / "ssl_key").glob("*.log"))), 1)
        self.assertEqual(len(list((output / "manifest").glob("*.json"))), 1)
        self.assertEqual(self.csv.read_bytes(), before)
        report = self.ingestor.verify_task_completeness(str(self.csv))
        self.assertEqual(report, {
            "expected_groups": 4, "matched_groups": 1, "missing_groups": 3,
            "expected_pages": 40, "matched_pages": 10,
        })

    def test_any_page_failure_rejects_entire_group(self):
        before = self.csv.read_bytes()
        self.make_result(self.jobs[0], failed=4)
        ok, error = self.ingestor.process_result(self.jobs[0], "test")
        self.assertFalse(ok)
        self.assertIn("batch_visit_failed=1/10", error)
        self.ingestor.on_task_failed(self.jobs[0], error)
        self.assertEqual(self.csv.read_bytes(), before)
        self.assertFalse(Path(self.ingestor.get_result_domain_base_dir(), "a.test").exists())

    def test_missing_artifact_does_not_move_other_artifact(self):
        self.make_result(self.jobs[0])
        workspace = Path(self.ingestor.HOST_CODE_PATH)
        (workspace / "ssl_key/group_ssl_key.log").unlink()
        self.assertFalse(self.ingestor.process_result(self.jobs[0], "test")[0])
        self.assertTrue((workspace / "data/group.pcap").is_file())

    def test_failed_group_status_rejects_all_successful_page_records(self):
        result = self.make_result(self.jobs[0])
        result["status"] = "fail"
        Path(self.ingestor.HOST_CODE_PATH, "meta/test_last.json").write_text(json.dumps(result), encoding="utf-8")
        self.assertFalse(self.ingestor.process_result(self.jobs[0], "test")[0])

    def test_wrong_group_result_is_rejected(self):
        self.make_result(self.jobs[0])
        self.assertFalse(self.ingestor.process_result(self.jobs[1], "test")[0])

    def test_task_list_transferred_by_file(self):
        task = dict(self.jobs[0], container="test", container_attempt=2,
                    container_history=["previous", "test"])
        self.ingestor._wait_before_first_exec = Mock()
        self.ingestor.run_cmd = Mock(return_value=subprocess.CompletedProcess([], 0))
        self.ingestor.process_result = Mock(return_value=(True, ""))
        self.ingestor._browser_label = "chrome153"
        self.assertTrue(self.ingestor.exec_once(task)[0])
        cmd = self.ingestor.run_cmd.call_args.args[0]
        self.assertEqual(cmd[-2:], ["--payload-file", "/app/meta/test_task.json"])
        payload = json.loads(Path(self.ingestor.HOST_CODE_PATH, "meta/test_task.json").read_text())
        self.assertEqual(len(payload["urls"]), 10)
        self.assertEqual(payload["group_id"], "batch_0001")
        self.assertEqual(payload["browser_label"], "chrome153")
        self.assertEqual(payload["page_wait_seconds"], 120)
        self.assertEqual(payload["page_gap_seconds"], 15)
        self.assertEqual(payload["container_attempt"], 2)
        self.assertEqual(payload["container_history"], ["previous", "test"])
        self.assertEqual(task["browser_label"], "chrome153")

    def test_http_404_page_failure_is_not_retried(self):
        self.make_result(self.jobs[0])
        meta = Path(self.ingestor.HOST_CODE_PATH, "meta/test_last.json")
        result = json.loads(meta.read_text())
        result["status"] = "fail"
        result["visited_urls"][4].update(
            {"status": "fail", "error": "http_error", "http_status": 404}
        )
        meta.write_text(json.dumps(result), encoding="utf-8")
        ok, error = self.ingestor.process_result(self.jobs[0], "test")
        self.assertFalse(ok)
        self.assertIn("batch_visit_failed=1/10", error)
        self.assertIn("HTTP_STATUS=404", error)
        self.assertIn("PAGE_NOT_FOUND", error)
        self.assertTrue(self.ingestor.is_non_retryable_error(error))

    def test_human_verification_failure_keeps_retry(self):
        self.make_result(self.jobs[0])
        meta = Path(self.ingestor.HOST_CODE_PATH, "meta/test_last.json")
        result = json.loads(meta.read_text())
        result["status"] = "fail"
        result["visited_urls"][2].update(
            {"status": "fail", "error": "human_verification", "http_status": 200}
        )
        meta.write_text(json.dumps(result), encoding="utf-8")
        ok, error = self.ingestor.process_result(self.jobs[0], "test")
        self.assertFalse(ok)
        self.assertIn("human_verification", error)
        self.assertFalse(self.ingestor.is_non_retryable_error(error))

    def test_artifact_error_from_group_is_surfaced(self):
        self.make_result(self.jobs[0])
        meta = Path(self.ingestor.HOST_CODE_PATH, "meta/test_last.json")
        result = json.loads(meta.read_text())
        result["status"] = "fail"
        result["error"] = "PCAP_INVALID=truncated"
        meta.write_text(json.dumps(result), encoding="utf-8")
        ok, error = self.ingestor.process_result(self.jobs[0], "test")
        self.assertFalse(ok)
        self.assertIn("PCAP_INVALID", error)

    def test_incomplete_group_is_rejected(self):
        with self.csv.open("a", encoding="utf-8") as file:
            file.write("11,https://a.test/11,a.test\n")
        with self.assertRaises(FatalIngestorError):
            self.cls().fetch_jobs()

    def test_duplicate_page_in_group_is_rejected(self):
        text = self.csv.read_text(encoding="utf-8")
        self.csv.write_text(text.replace("2,https://a.test/2", "1,https://a.test/1"), encoding="utf-8")
        with self.assertRaises(FatalIngestorError):
            self.cls().fetch_jobs()

    def test_batch_runs_only_once_even_when_csv_is_preserved(self):
        with patch("host_scheduler.single_csv_profiles.run_with_policy") as runner:
            run_profile(self.definition, self.cls)
        self.assertEqual(runner.call_args.args[1], BATCH_RUN_POLICY)

    def test_failed_group_retries_all_ten_pages_as_one_task(self):
        tasks = queue.Queue()
        tasks.put(self.jobs[0])
        stats = {"ok": 0, "fail": 0, "errors": []}
        self.ingestor.exec_once = Mock(side_effect=[(False, "page failed"), (True, "")])
        self.ingestor._update_progress = Mock()
        with patch("host_scheduler.base_traffic_ingestor.time.sleep"):
            self.ingestor.worker_loop("test", tasks, stats, retry=1)
        self.assertEqual(stats, {"ok": 1, "fail": 0, "errors": []})
        self.assertEqual(self.ingestor.exec_once.call_count, 2)
        for call in self.ingestor.exec_once.call_args_list:
            self.assertEqual(len(call.args[0]["urls"]), 10)
            self.assertEqual(call.args[0]["group_id"], "batch_0001")

    def run_container_retry_case(self, retry):
        attempts = []

        def execute(task):
            attempts.append((task["container"], task["container_attempt"],
                             list(task["container_history"])))
            # 让其它 worker 先看到空队列，覆盖尾部任务重试的场景。
            time.sleep(0.15)
            return (len(attempts) > retry, "page failed")

        self.ingestor.RETRY = retry
        self.ingestor.exec_once = execute
        self.ingestor._update_progress = Mock()
        stats = self.ingestor.run_once(["c1", "c2", "c3"], [self.jobs[0]])
        self.assertEqual(stats, {"ok": 1, "fail": 0, "errors": []})
        self.assertEqual(len(attempts), retry + 1)
        self.assertEqual(self.ingestor._retry_container_pool, ())
        for index, (_, attempt, history) in enumerate(attempts, 1):
            self.assertEqual(attempt, index)
            self.assertEqual(history, [entry[0] for entry in attempts[:index]])
        return [entry[0] for entry in attempts]

    def test_retry_waits_for_an_unused_container_after_queue_is_empty(self):
        containers = self.run_container_retry_case(retry=1)
        self.assertNotEqual(containers[0], containers[1])

    def test_retry_reuses_containers_only_after_pool_is_exhausted(self):
        containers = self.run_container_retry_case(retry=4)
        self.assertEqual(len(set(containers[:3])), 3)
        self.assertEqual(len(containers), 5)

    def test_normal_firefox_profile_still_uses_single_row_tasks(self):
        definition = load_profile_definition(ROOT / "trafficIngestor/single_csv/firefox.py")
        cls = build_ingestor(definition, __name__)
        self.assertIs(cls.fetch_jobs, CsvTaskSourceMixin.fetch_jobs)
        self.assertEqual(cls.ACTION_PROFILE, "tools/browsers/firefox.py")

    def test_successful_group_rows_are_reserved_for_removal(self):
        ingestor = self.cls()
        ingestor.log = Mock()
        group = self.jobs[0]
        ingestor.on_task_success(group, {"pcap": "/tmp/group.pcap"})
        journal = Path(ingestor.BASE_DST, ".input.csv.journal")
        self.assertTrue(journal.is_file())
        entries = [json.loads(line) for line in journal.read_text().splitlines()]
        self.assertEqual(entries[0]["kind"], "csv-removal-journal")
        self.assertEqual(
            [(row["id"], row["domain"]) for row in entries[1:]],
            [(str(index), "a.test") for index in range(1, 11)],
        )
        # 只登记了本轮 a.test 的 10 行，另一轮 a.test 与 b.test 仍可继续采集；
        # 组号按该域名剩余行在 CSV 中的先后重新编号。
        remaining = self.cls().fetch_jobs()
        self.assertEqual(
            [(job["domain"], job["group_id"]) for job in remaining],
            [("b.test", "batch_0001"), ("b.test", "batch_0002"), ("a.test", "batch_0001")],
        )
        self.assertEqual(
            [row["url"] for row in remaining[-1]["urls"]],
            [f"https://a.test/{index}" for index in range(1, 11)],
        )
        _, rows = ingestor._read_csv_records(self.csv)
        self.assertEqual(len(rows), 40)


class BatchActionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 只替换本机缺失的浏览器/系统依赖，执行真实批量流程和密钥整理方法。
        modules = {}
        for name in ("tools", "tools.browsers", "selenium", "selenium.webdriver", "selenium.webdriver.support",
                     "selenium.webdriver.support.ui", "tools.capture", "tools.logger",
                     "tools.browsers.chrome"):
            modules[name] = types.ModuleType(name)
        modules["tools"].__path__ = [str(ROOT / "trafficIngestor/tools")]
        modules["tools.browsers"].__path__ = [str(ROOT / "trafficIngestor/tools/browsers")]
        modules["selenium.webdriver.support.ui"].WebDriverWait = Mock()
        for name in ("capture", "stop_capture", "kill_tcpdump_processes"):
            setattr(modules["tools.capture"], name, Mock())
        modules["tools.logger"].setup_logging = Mock(return_value=Mock())
        for name in ("create_chrome_driver", "kill_chrome_processes",
                     "build_browser_error_diagnostics", "open_url_and_save_content",
                     "get_main_document_response"):
            setattr(modules["tools.browsers.chrome"], name, Mock())
        cls.dependencies = patch.dict(sys.modules, modules)
        cls.dependencies.start()
        cls.addClassCleanup(cls.dependencies.stop)
        spec = importlib.util.spec_from_file_location("tested_batch_action", ROOT / "trafficIngestor/tools/action_batch.py")
        cls.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.module)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.payload = {
            "container": "test", "domain": "a.test", "group_id": "batch_0001",
            "urls": [{"row_id": str(i), "url": f"https://a.test/{i}"} for i in range(10)],
        }

    OK_PAGE = {"failure_reason": "", "http_status": 200, "title": "page"}

    def prepare_artifacts(self):
        browser = Mock()
        browser.current_url = "https://a.test/1"
        browser.page_source = "<html><body>ok</body></html>"
        source = self.root / "ssl_key_tmp/test.log"
        source.parent.mkdir()
        source.write_text("real test keylog", encoding="utf-8")
        final = self.root / "ssl_key/test.log"
        pcap = self.root / "group.pcap"
        pcap.write_bytes(
            struct.pack("<IHHIIII", 0xa1b2c3d4, 2, 4, 0, 0, 65535, 1)
            + struct.pack("<IIII", 1, 0, 4, 4) + b"test"
        )
        browser._traffic_ingestor_ssl_key_source_path = str(source)
        browser._traffic_ingestor_ssl_key_final_path = str(final)
        return browser, source, final, pcap

    def execute(self, visit_results=None, fail_create=False, pcap_path=None):
        browser, source, final, pcap = self.prepare_artifacts()
        if visit_results is None:
            visit_results = [dict(self.OK_PAGE) for _ in range(10)]
        with patch.object(self.module, "_project_root", str(self.root)), \
             patch.object(self.module.time, "sleep"), \
             patch.object(self.module.BatchAction, "_start_reaper"), \
             patch.object(self.module, "capture") as capture, \
             patch.object(self.module, "stop_capture",
                          return_value=str(pcap_path or pcap)) as stop, \
             patch.object(self.module, "create_chrome_driver",
                          return_value=(browser, str(final))) as create:
            action = self.module.BatchAction()
            action.visit_url = Mock(side_effect=visit_results)
            if fail_create:
                create.side_effect = RuntimeError("driver failed")
            result = action.start_batch_task(self.payload)
            capture.assert_called_once()
            create.assert_called_once()
            stop.assert_called_once()
            self.assertEqual(capture.call_args.args[2], "batch_0001")
            self.assertEqual(capture.call_args.kwargs["data_base_dir"], str(self.root))
            self.assertTrue(
                Path(self.root, "meta/test_last.json").is_file()
            )
            if not fail_create:
                self.assertEqual(final.read_text(), "real test keylog")
                self.assertFalse(source.exists())
            return result, action, capture

    def test_ten_pages_share_capture_browser_and_final_keylog(self):
        result, action, _ = self.execute()
        self.assertEqual(action.visit_url.call_count, 10)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["url_count"], 10)
        self.assertEqual(len(result["visited_urls"]), 10)
        self.assertTrue(all(row["status"] == "ok" for row in result["visited_urls"]))
        self.assertEqual(result["pcap_size"], Path(result["pcap_path"]).stat().st_size)

    def test_one_failed_page_marks_whole_group_failed_and_stops_visiting(self):
        result, action, _ = self.execute(
            visit_results=[RuntimeError("page failed")] + [dict(self.OK_PAGE)] * 9
        )
        self.assertEqual(result["status"], "fail")
        self.assertIn("page failed", result["error"])
        self.assertEqual(action.visit_url.call_count, 1)
        self.assertEqual(len(result["visited_urls"]), 1)
        self.assertEqual(result["visited_urls"][0]["status"], "fail")

    def test_http_error_page_fails_group_at_that_page(self):
        bad_page = {"failure_reason": "http_error", "http_status": 404, "title": "404"}
        result, action, _ = self.execute(
            visit_results=[dict(self.OK_PAGE), dict(self.OK_PAGE), bad_page]
        )
        self.assertEqual(result["status"], "fail")
        self.assertIn("http_error", result["error"])
        self.assertEqual(action.visit_url.call_count, 3)
        self.assertEqual(result["visited_urls"][-1]["http_status"], 404)
        self.assertEqual(result["visited_urls"][-1]["status"], "fail")

    def test_human_verification_page_fails_group(self):
        bad_page = {"failure_reason": "human_verification", "http_status": 200,
                    "human_verification_signals": ["text:captcha"]}
        result, _, _ = self.execute(visit_results=[bad_page])
        self.assertEqual(result["status"], "fail")
        self.assertIn("human_verification", result["error"])
        self.assertEqual(
            result["visited_urls"][0]["human_verification_signals"], ["text:captcha"]
        )

    def test_driver_startup_failure_still_stops_capture(self):
        result, _, _ = self.execute(fail_create=True)
        self.assertEqual(result["status"], "fail")
        self.assertIn("driver failed", result["error"])
        self.assertEqual(result["visited_urls"], [])

    def test_invalid_pcap_fails_group_without_raising(self):
        broken = self.root / "broken.pcap"
        broken.write_bytes(b"not a pcap")
        result, _, _ = self.execute(pcap_path=broken)
        self.assertEqual(result["status"], "fail")
        self.assertIn("PCAP_INVALID", result["error"])

    def test_payload_carries_page_timing(self):
        self.payload["page_wait_seconds"] = 5
        self.payload["page_gap_seconds"] = 1
        result, action, _ = self.execute()
        self.assertEqual(result["page_wait_seconds"], 5)
        self.assertEqual(result["page_gap_seconds"], 1)
        self.assertEqual(action.visit_url.call_args.kwargs["wait_secs"], 5)

    def test_inspect_page_falls_back_to_main_document_response(self):
        driver = Mock()
        driver.current_url = "https://a.test/1"
        driver.page_source = "<html><body>ok</body></html>"
        driver.execute_script.return_value = {}
        with patch.object(self.module.BatchAction, "_start_reaper"):
            action = self.module.BatchAction()
        action.logger = Mock()
        with patch.object(self.module, "get_main_document_response",
                          return_value={"status": 403, "url": "https://a.test/1"}):
            validation = action.inspect_page(driver, "https://a.test/1")
        self.assertEqual(validation["http_status"], 403)
        self.assertEqual(validation["failure_reason"], "http_error")

    def test_inspect_page_detects_challenge_html(self):
        driver = Mock()
        driver.current_url = "https://a.test/1"
        driver.page_source = "<html><body>g-recaptcha</body></html>"
        driver.execute_script.return_value = {"title": "Just a moment"}
        with patch.object(self.module.BatchAction, "_start_reaper"):
            action = self.module.BatchAction()
        action.logger = Mock()
        with patch.object(self.module, "get_main_document_response",
                          return_value={"status": 200, "url": "https://a.test/1"}):
            validation = action.inspect_page(driver, "https://a.test/1")
        self.assertTrue(validation["human_verification"])
        self.assertEqual(validation["failure_reason"], "human_verification")

    def test_relocated_action_uses_mount_root(self):
        target = self.root / "action.py"
        target.write_text(Path(self.module.__file__).read_text(encoding="utf-8"), encoding="utf-8")
        spec = importlib.util.spec_from_file_location("relocated_action", target)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.assertEqual(module._project_root, str(self.root))

    def test_cli_reads_payload_file_and_exits_nonzero_for_failed_group(self):
        path = self.root / "payload.json"
        path.write_text(json.dumps(self.payload), encoding="utf-8")
        with patch.object(sys, "argv", ["action.py", "--payload-file", str(path)]), \
             patch.object(self.module.BatchAction, "__init__", return_value=None), \
             patch.object(self.module.BatchAction, "start_batch_task", return_value={"status": "fail"}) as start:
            with self.assertRaises(SystemExit) as error:
                self.module.BatchAction.run_from_argv()
            self.assertEqual(error.exception.code, 1)
            self.assertEqual(start.call_args.args[0], self.payload)

    def test_cli_exits_zero_for_successful_group(self):
        path = self.root / "payload.json"
        path.write_text(json.dumps(self.payload), encoding="utf-8")
        with patch.object(sys, "argv", ["action.py", "--payload-file", str(path)]), \
             patch.object(self.module.BatchAction, "__init__", return_value=None), \
             patch.object(self.module.BatchAction, "start_batch_task",
                          return_value={"status": "ok"}):
            self.module.BatchAction.run_from_argv()


if __name__ == "__main__":
    unittest.main()
