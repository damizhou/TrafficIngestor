import os


class Task:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Task, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self.row_id = ''
            self.url = ''
            self.domain = ''
            self.container = 'unknown'
            self.requesturlNum = 0
            self.collected_urls = []
            self.exclude_keywords = []
            self._initialized = True

    def init_from_payload(self, payload: dict) -> None:
        """从 get_url_list.py 传入的 JSON payload 初始化。

        payload: {"row_id": "1", "url": "https://example.com", "domain": "example.com", "container": "xxx"}
        """
        self.row_id = str(payload.get('row_id', '')).strip()
        self.url = (payload.get('url', '') or '').strip()
        self.domain = (payload.get('domain', '') or '').strip()
        self.container = str(payload.get('container', 'unknown')).strip() or 'unknown'
        self.requesturlNum = 0
        self.collected_urls = []

        # exclude_keywords: 文件存在则读取，否则空列表
        exclude_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'exclude_keywords'
        )
        if os.path.exists(exclude_path):
            try:
                with open(exclude_path, 'r') as f:
                    self.exclude_keywords = [s.strip() for s in f.readlines() if s.strip()]
            except Exception:
                self.exclude_keywords = []

    @property
    def current_start_url(self):
        """兼容 TraceSpider 类定义时读取 start_urls。"""
        return self.url

    @property
    def current_allowed_domain(self):
        """兼容 TraceSpider 类定义时读取 allowed_domains。"""
        return self.domain


task_instance = Task()
