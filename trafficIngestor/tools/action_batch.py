"""按域名分组的采集入口：同站点的一批页面共享一份 pcap 和一份 keylog。

与 ``traffic_capture_single_csv/action.py`` 的单页流程相比，本入口只负责“一组页面
一次抓包”，不保存正文、HTML 和截图；组内任意页面失败都会让整组失败，由宿主机
放弃迁移产物并保留 CSV 记录，等待下一轮按组重试。
"""

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from selenium.webdriver.support.ui import WebDriverWait

# 复制成容器内 /app/action.py 后，本文件所在目录就是挂载根目录。
_current_dir: str = os.path.dirname(os.path.abspath(__file__))
_project_root: str = (
    os.path.dirname(_current_dir) if Path(_current_dir).name == "tools" else _current_dir
)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from tools.base_action import BaseAction
from tools.browsers.chrome import (
    create_chrome_driver,
    get_main_document_response,
    kill_chrome_processes,
)
from tools.capture import capture, kill_tcpdump_processes, stop_capture
from tools.logger import setup_logging
from tools.pcap_validation import validate_pcap_structure


# 每个页面的驻留时间与页面间隔，可由宿主机 profile 通过任务 JSON 覆盖。
DEFAULT_PAGE_WAIT_SECONDS = 120
DEFAULT_PAGE_GAP_SECONDS = 15
DEFAULT_READY_STATE_TIMEOUT = 30
# Bound a renderer stuck inside driver.get(); the host-side docker timeout is
# intentionally much larger for a normal ten-page group.
DEFAULT_PAGE_LOAD_TIMEOUT = 180
# 正常结束后等待 TCP 挥手完成，失败组直接跳过以尽早重试。
TCP_FINISH_WAIT_SECONDS = 60


class BatchAction(BaseAction):
    """同域名多页共享抓包的批量任务 Action。"""

    browser_name = "Chrome"

    def traffic(self, domain, formatted_time, group_id="batch", artifact_label=None):
        """启动一次覆盖整组页面的抓包。"""
        return capture(
            domain,
            formatted_time,
            group_id,
            data_base_dir=_project_root,
            logger=self.logger,
            exclude_hosts=self.get_capture_exclude_hosts(),
            artifact_label=artifact_label,
        )

    def visit_url(self, driver, url, wait_secs=DEFAULT_PAGE_WAIT_SECONDS,
                  ready_timeout=DEFAULT_READY_STATE_TIMEOUT):
        """访问页面并在驻留时间结束后校验页面状态。"""
        driver.set_page_load_timeout(DEFAULT_PAGE_LOAD_TIMEOUT)
        driver.get(url)
        WebDriverWait(driver, ready_timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        if wait_secs > 0:
            time.sleep(wait_secs)
        return self.inspect_page(driver, url)

    @staticmethod
    def read_page_source(driver):
        """读取当前页源码，失败时返回空串而不是中断整组。"""
        try:
            return driver.page_source or ""
        except Exception:
            return ""

    def inspect_page(self, driver, requested_url):
        """返回页面状态码与人机校验结果，供组内判定是否继续。"""
        current_url = ""
        try:
            current_url = driver.current_url or ""
        except Exception:
            pass

        validation = self.inspect_loaded_page(
            driver,
            requested_url,
            current_url,
            "",
            "",
            page_html=self.read_page_source(driver),
        )
        if validation.get("http_status"):
            return validation

        # Chromium 的 performance 导航条目不一定带状态码，和单页入口一样回退到主文档响应。
        try:
            response = get_main_document_response(
                driver,
                requested_url=requested_url,
                current_url=current_url,
            )
            status = int(response.get("status") or 0)
        except Exception as exc:
            self.logger.warning(
                f"HTTP 主文档状态码校验失败: {type(exc).__name__}: {exc}"
            )
            return validation

        if status:
            validation["http_status"] = status
            validation["response_url"] = str(
                response.get("url") or current_url or requested_url
            )
            if status >= 400:
                validation["failure_reason"] = "http_error"
        return validation

    def build_page_record(self, row_id, url, validation, current_url=""):
        """把一次页面访问整理成可写入 manifest 的记录。"""
        http_status = int(validation.get("http_status") or 0)
        failure_reason = str(validation.get("failure_reason") or "").strip()
        record = {
            "row_id": row_id,
            "url": url,
            "current_url": current_url or validation.get("response_url", ""),
            "http_status": http_status,
            "title": validation.get("title", ""),
            "status": "fail" if failure_reason else "ok",
        }
        if failure_reason:
            record["error"] = failure_reason
            signals = validation.get("human_verification_signals", [])
            if signals:
                record["human_verification_signals"] = list(signals)
        return record

    def start_batch_task(self, payload):
        """执行一组页面采集，任何失败都写入 status=fail 的结果文件。"""
        container = payload["container"]
        domain = payload["domain"]
        urls = payload["urls"]
        group_id = payload.get("group_id", "batch")
        page_wait_seconds = float(
            payload.get("page_wait_seconds", DEFAULT_PAGE_WAIT_SECONDS)
        )
        page_gap_seconds = float(
            payload.get("page_gap_seconds", DEFAULT_PAGE_GAP_SECONDS)
        )
        artifact_label = str(payload.get("browser_label", "") or "").strip()

        self.allowed_domain = domain
        self.container_name = container
        if self.logger is None:
            self.logger = setup_logging(container_name=container)

        meta_path = os.path.join(_project_root, "meta", f"{container}_last.json")
        self.clean_old_files(meta_path)
        # 上一次运行的 meta 不能被本次读取，否则宿主机可能误把旧结果当成本组成绩。
        if os.path.exists(meta_path):
            os.remove(meta_path)

        formatted_time = datetime.now().strftime("%Y%m%d_%H_%M_%S_%f")
        kill_chrome_processes()
        kill_tcpdump_processes()
        time.sleep(1)

        visited_urls = []
        pcap_path = ""
        ssl_key_file_path = ""
        group_error = ""
        browser = None

        self.logger.info(
            f"开始分组任务: domain={domain}, group={group_id}, url_count={len(urls)}, "
            f"page_wait={page_wait_seconds:g}s, page_gap={page_gap_seconds:g}s"
        )
        try:
            self.traffic(domain, formatted_time, group_id, artifact_label)
            time.sleep(1)

            self.logger.info("创建浏览器")
            browser, ssl_key_file_path = create_chrome_driver(
                domain,
                formatted_time,
                group_id,
                data_base_dir=_project_root,
                logger=self.logger,
                artifact_label=artifact_label,
            )

            for index, url_info in enumerate(urls):
                row_id = url_info["row_id"]
                url = url_info["url"]
                self.logger.info(f"访问第 {index + 1}/{len(urls)} 个页面 [{row_id}] {url}")
                try:
                    validation = self.visit_url(
                        browser,
                        url,
                        wait_secs=page_wait_seconds,
                    )
                    record = self.build_page_record(row_id, url, validation)
                except Exception as exc:
                    self.logger.error(f"访问 [{row_id}] {url} 失败: {exc}")
                    record = {
                        "row_id": row_id,
                        "url": url,
                        "status": "fail",
                        "error": f"{type(exc).__name__}: {exc}"[:500],
                    }
                visited_urls.append(record)

                if record["status"] != "ok":
                    # 一组共用一份抓包，后面的页面成功也无法产出可用的组结果。
                    group_error = (
                        f"page_failed row_id={row_id} url={url} "
                        f"reason={record.get('error', 'unknown')}"
                    )
                    self.logger.error(f"组内页面失败，终止本组剩余页面: {group_error}")
                    break
                if index + 1 < len(urls) and page_gap_seconds > 0:
                    time.sleep(page_gap_seconds)
        except Exception as exc:
            group_error = f"{type(exc).__name__}: {exc}"
            self.logger.error(f"分组任务执行异常: {group_error}")
        finally:
            try:
                if browser is not None:
                    try:
                        browser.quit()
                    except Exception as exc:
                        self.logger.warning(f"browser.quit() 异常: {exc}")
                kill_chrome_processes()
            finally:
                if group_error or not visited_urls:
                    failure_reason = group_error or "no_page_visited"
                    self.logger.info(
                        f"本组未成功，domain={domain}，reason={failure_reason}，"
                        "跳过等待 TCP 结束挥手完成"
                    )
                else:
                    self.logger.info(
                        f"等待TCP结束挥手完成，耗时{TCP_FINISH_WAIT_SECONDS}秒"
                    )
                    time.sleep(TCP_FINISH_WAIT_SECONDS)
                self.logger.info("关流量收集")
                pcap_path = stop_capture() or ""

        if browser is not None:
            try:
                ssl_key_file_path = self.finalize_ssl_key_file(
                    browser, ssl_key_file_path
                )
            except Exception as exc:
                group_error = group_error or (
                    f"SSL keylog 最终复制异常: {type(exc).__name__}: {exc}"
                )
                self.logger.error(group_error)

        if not group_error:
            valid, error = validate_pcap_structure(pcap_path)
            if not valid:
                group_error = f"PCAP_INVALID={error}"
            elif not ssl_key_file_path or not os.path.exists(ssl_key_file_path):
                group_error = f"SSL_KEY_MISSING={ssl_key_file_path}"
            elif os.path.getsize(ssl_key_file_path) <= 0:
                group_error = f"SSL_KEY_EMPTY={ssl_key_file_path}"

        pcap_file_size = (
            os.path.getsize(pcap_path)
            if pcap_path and os.path.exists(pcap_path)
            else 0
        )
        ssl_key_file_size = (
            os.path.getsize(ssl_key_file_path)
            if ssl_key_file_path and os.path.exists(ssl_key_file_path)
            else 0
        )
        self.logger.info(
            f"pcap文件大小：{pcap_file_size}，ssl_key文件大小：{ssl_key_file_size}"
        )

        result = {
            "domain": domain,
            "group_id": group_id,
            "container_attempt": payload.get("container_attempt", 1),
            "container_history": payload.get("container_history", [container]),
            "status": "ok" if not group_error else "fail",
            "error": group_error,
            "pcap_path": pcap_path,
            "ssl_key_file_path": ssl_key_file_path or "",
            "url_count": len(urls),
            "visited_count": len(visited_urls),
            "visited_urls": visited_urls,
            "page_wait_seconds": page_wait_seconds,
            "page_gap_seconds": page_gap_seconds,
            "pcap_size": pcap_file_size,
            "ssl_key_size": ssl_key_file_size,
        }
        self.write_result(meta_path, result)

        if result["status"] == "ok":
            self.logger.info(
                f"分组任务完成: domain={domain}, group={group_id}, "
                f"pages={len(visited_urls)}"
            )
        else:
            self.logger.error(
                f"分组任务失败: domain={domain}, group={group_id}, "
                f"pages={len(visited_urls)}/{len(urls)}, error={group_error}"
            )
        time.sleep(1)
        return result

    @classmethod
    def run_from_argv(cls):
        """命令行入口，失败组以非零码退出，便于宿主机按组重试。"""
        if len(sys.argv) < 2:
            print(
                "Usage: python action_batch.py '<json_payload>' | --payload-file <path>"
            )
            sys.exit(1)

        if sys.argv[1] == "--payload-file":
            if len(sys.argv) != 3:
                raise ValueError("--payload-file 需要一个 JSON 文件路径")
            with open(sys.argv[2], encoding="utf-8") as file:
                payload = json.load(file)
        else:
            payload = json.loads(sys.argv[1])

        action = cls()
        try:
            result = action.start_batch_task(payload)
        except Exception as exc:
            message = f"分组任务异常退出: {type(exc).__name__}: {exc}"
            if action.logger is not None:
                action.logger.error(message)
            else:
                print(message, file=sys.stderr)
            sys.exit(1)
        if result.get("status") != "ok":
            sys.exit(1)


if __name__ == "__main__":
    BatchAction.run_from_argv()
