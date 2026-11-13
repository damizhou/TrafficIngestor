#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""容器内单页访问：打开一个 URL → 校验 → 提取页面链接 → 以一行 JSON 输出结果。

用法（由宿主机 urlFind/visit.py 通过 docker exec 调用）：
    python /app/urlFind/worker/visit_page.py '<json payload>'
    python /app/urlFind/worker/visit_page.py --selftest

payload: {"visit_id", "url", "page_load_timeout", "settle_seconds", "max_links"}

容器同一时刻只执行一个访问（由宿主机调度保证），所以开始前可以安全清理残留 Chrome。
本文件运行在容器的 Python 3.9 中。
"""

from __future__ import annotations

import glob
import json
import os
import shutil
import subprocess
import sys
import time

# /app/urlFind/worker/visit_page.py -> /app
_SOURCE_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _SOURCE_ROOT not in sys.path:
    sys.path.insert(0, _SOURCE_ROOT)

from urlFind.protocol import (  # noqa: E402
    OUTCOME_ERROR, OUTCOME_OK, OUTCOME_REJECTED, OUTCOME_TIMEOUT, RESULT_MARKER,
)

# 一次 JS 调用取出所有 <a href>（浏览器已解析为绝对地址）
LINKS_SCRIPT = "return [...document.querySelectorAll('a[href]')].map(a => a.href);"
# Selenium/Chrome 在 /tmp 下留下的临时 profile 目录
CHROME_TMP_PATTERNS = ("/tmp/.com.google.Chrome.*", "/tmp/.org.chromium.Chromium.*", "/tmp/scoped_dir*")


def log(msg: str) -> None:
    """诊断信息写 stderr，stdout 只留给结果行。"""
    print(f"[visit_page] {msg}", file=sys.stderr, flush=True)


def kill_browsers() -> None:
    # 按进程名精确匹配；不用 -f，避免命中命令行里带 "chrome" 的本进程（payload 含 URL）。
    for name in ("chromedriver", "google-chrome", "chrome"):
        subprocess.run(["pkill", "-9", "-x", name], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL, check=False)


def clean_chrome_tmp() -> None:
    for pattern in CHROME_TMP_PATTERNS:
        for path in glob.glob(pattern):
            shutil.rmtree(path, ignore_errors=True)


def emit(result: dict) -> None:
    print(RESULT_MARKER + json.dumps(result, ensure_ascii=False), flush=True)


def extract_page_links(browser, max_links: int, is_page_url) -> list:
    raw = browser.execute_script(LINKS_SCRIPT) or []
    links = []
    seen = set()
    for item in raw:
        if not isinstance(item, str):
            continue
        url = item.strip().split("#", 1)[0]
        if not url or url in seen or not is_page_url(url):
            continue
        seen.add(url)
        links.append(url)
        if len(links) >= max_links:
            break
    return links


def visit(payload: dict) -> dict:
    from selenium.common.exceptions import TimeoutException, WebDriverException
    from url_list_collector.utils.chrome import create_chrome_driver
    from url_list_collector.utils.page_validation import (
        PageRejected, is_page_url, read_validated_page,
    )

    started = time.time()
    url = str(payload.get("url", "")).strip()
    result = {
        "visit_id": payload.get("visit_id", ""),
        "request_url": url,
        "final_url": "",
        "outcome": OUTCOME_ERROR,
        "http_status": None,
        "reason": "",
        "links": [],
        "elapsed": 0.0,
    }

    kill_browsers()
    clean_chrome_tmp()
    browser = None
    try:
        browser = create_chrome_driver()
        browser.set_page_load_timeout(int(payload.get("page_load_timeout", 60)))
        browser.get_log("performance")  # 清空启动阶段的事件
        try:
            browser.get(url)
        except TimeoutException:
            result["outcome"] = OUTCOME_TIMEOUT
            result["reason"] = "page_load_timeout"
            return result
        time.sleep(float(payload.get("settle_seconds", 15)))
        try:
            snapshot, status = read_validated_page(browser)
        except PageRejected as exc:
            result["outcome"] = OUTCOME_REJECTED
            result["reason"] = str(exc)
            try:
                result["final_url"] = browser.current_url
            except WebDriverException:
                pass
            return result
        result["final_url"] = snapshot["url"]
        result["http_status"] = status
        result["links"] = extract_page_links(browser, int(payload.get("max_links", 2000)), is_page_url)
        result["outcome"] = OUTCOME_OK
        return result
    except WebDriverException as exc:
        first_line = (getattr(exc, "msg", None) or str(exc)).strip().splitlines()
        result["reason"] = f"browser_error:{type(exc).__name__}:{first_line[0][:200] if first_line else ''}"
        return result
    except Exception as exc:  # 兜底：任何异常都要产出结果行
        result["reason"] = f"worker_error:{type(exc).__name__}:{str(exc)[:200]}"
        return result
    finally:
        if browser is not None:
            try:
                browser.quit()
            except Exception:
                pass
        kill_browsers()
        clean_chrome_tmp()
        result["elapsed"] = round(time.time() - started, 2)


def selftest() -> int:
    """检查容器内依赖是否齐全，不启动浏览器。"""
    from selenium import webdriver  # noqa: F401
    from url_list_collector.utils.chrome import create_chrome_driver  # noqa: F401
    from url_list_collector.utils.page_validation import read_validated_page  # noqa: F401
    emit({"selftest": "ok", "python": sys.version.split()[0]})
    return 0


def main() -> int:
    if len(sys.argv) != 2:
        log("usage: visit_page.py '<json payload>' | --selftest")
        return 2
    if sys.argv[1] == "--selftest":
        return selftest()
    payload = json.loads(sys.argv[1])
    result = visit(payload)
    log(f"{result['outcome']} {result['reason']} {result['request_url']} -> {result['final_url']} "
        f"links={len(result['links'])} elapsed={result['elapsed']}s")
    emit(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
