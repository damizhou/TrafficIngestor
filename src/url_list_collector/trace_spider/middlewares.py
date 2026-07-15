# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import time
from scrapy import signals
from scrapy.http import HtmlResponse
from scrapy.exceptions import IgnoreRequest
from urllib.parse import urlparse
from utils.logger import logger
from utils.chrome import create_chrome_driver
from utils.task import task_instance



def _normalize_url(raw_url: str) -> str:
    return (raw_url or "").strip().split("#", 1)[0]


def _is_same_site(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower().strip(".")
    base = (task_instance.current_allowed_domain or "").lower().strip(".")
    if not host or not base:
        return False
    return host == base or host.endswith("." + base)


def _should_collect(url: str) -> bool:
    if not url.startswith(("http://", "https://")):
        return False
    if not _is_same_site(url):
        return False
    if "/cdn-cgi/" in url or "challenge-platform" in url:
        return False
    if any(keyword in url for keyword in task_instance.exclude_keywords):
        return False
    return True


def _collect_url(url: str) -> None:
    normalized = _normalize_url(url)
    if not normalized:
        return
    if not _should_collect(normalized):
        return
    if normalized not in task_instance.collected_urls:
        task_instance.collected_urls.append(normalized)


class TraceSpiderSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class TraceSpiderDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def __init__(self):
        logger.info(f"创建浏览器")
        self.browser = create_chrome_driver()
        if 'youtube' in task_instance.current_allowed_domain:
            logger.info(f"cookies开始加载")
            self.browser.get('https://www.youtube.com/')
            # Retrieve all cookies

            self.browser.refresh()  # 带 cookie 重载
            logger.info("cookies加载完成")

    def __del__(self):
        logger.info(f"销毁浏览器")
        browser = getattr(self, "browser", None)
        if browser:
            browser.close()

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        # 打印页面内容
        if task_instance.requesturlNum >= task_instance.target_urls_per_domain:
            raise IgnoreRequest(
                f"超过{task_instance.target_urls_per_domain}个页面限制，忽略请求"
            )

        self.browser.get(request.url)
        try:
            task_instance.requesturlNum += 1
            logger.info(f"requestURL:{self.browser.current_url}")
            _collect_url(self.browser.current_url)
            time.sleep(15)
        except Exception as e:
            logger.error(f"爬取 {request.url} 失败: {e}")

        return HtmlResponse(url=request.url, body=self.browser.page_source, encoding='utf-8', request=request)

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)
