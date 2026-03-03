import json
import os
import subprocess
import sys
import time
from utils.task import task_instance
from utils.logger import logger, set_log_container
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

# 容器内工作目录
_current_dir = os.path.dirname(os.path.abspath(__file__))
# 显式指定 Scrapy 配置模块，避免在无 scrapy.cfg 场景下丢失项目设置
os.environ.setdefault("SCRAPY_SETTINGS_MODULE", "trace_spider.settings")


def kill_chrome_processes():
    """清除浏览器进程"""
    try:
        subprocess.run(['pkill', '-f', 'chromedriver'], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        subprocess.run(['pkill', '-f', 'google-chrome'], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e.stderr.decode('utf-8')}")


def write_result(container: str, result: dict) -> None:
    """将结果写入 meta/{container}_last.json，供 get_url_list.py 的 process_result 读取。"""
    meta_dir = os.path.join(_current_dir, "meta")
    os.makedirs(meta_dir, exist_ok=True)
    meta_path = os.path.join(meta_dir, f"{container}_last.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python action.py '<json_payload>'")
        sys.exit(1)

    # 1. 解析 JSON payload: {"row_id", "url", "domain", "container"}
    payload = json.loads(sys.argv[1])

    # 2. 初始化 task_instance（必须在导入 TraceSpider 之前，因为 Spider 类定义时会读取 task_instance 属性）
    task_instance.init_from_payload(payload)

    # 延迟导入，确保 TraceSpider 类属性 start_urls/allowed_domains 读取到当前 payload
    from trace_spider.spiders.trace import TraceSpider

    container = task_instance.container
    domain = task_instance.domain
    url = task_instance.url

    # 绑定当前任务容器日志文件，并透传给 Scrapy 日志配置
    set_log_container(container)
    os.environ["TRACE_LOG_CONTAINER"] = container

    logger.info(f"开始任务 row_id={task_instance.row_id} domain={domain} url={url}")

    # 4. 清理浏览器进程
    logger.info("清理浏览器进程")
    kill_chrome_processes()

    # 5. 启动爬虫
    process = CrawlerProcess(get_project_settings())
    process.crawl(TraceSpider)

    logger.info("开始爬取数据")
    error = ""
    try:
        process.start()
    except Exception as e:
        error = str(e)[:500]
        logger.exception(f"爬取异常: {e}")

    # 6. 等待浏览器加载完成
    logger.info("爬取数据结束, 等待10秒让浏览器加载完所有已请求的页面")
    time.sleep(10)

    # 7. 清理浏览器进程
    logger.info("清理浏览器进程")
    kill_chrome_processes()

    # 兜底：若未采集到任何 URL，至少保留种子 URL 以避免空结果
    if not task_instance.collected_urls and url:
        task_instance.collected_urls.append(url)
        logger.warning("未采集到有效子链接，回退写入种子URL")

    logger.info(
        f"{url} 流量收集结束，共爬取 {task_instance.requesturlNum} 个页面，"
        f"采集 {len(task_instance.collected_urls)} 条 URL"
    )

    # 9. 写入结果到 meta/{container}_last.json
    result = {
        "row_id": task_instance.row_id,
        "domain": domain,
        "collected_urls": task_instance.collected_urls,
        "visited_count": task_instance.requesturlNum,
        "error": error,
    }
    write_result(container, result)

    # 固定返回 0，交由 get_url_list.py 根据 meta 判断是否重试
    sys.exit(0)


if __name__ == "__main__":
    main()
