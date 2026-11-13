"""宿主机与容器之间的单页访问协议（宿主机 Python 3.12 与容器 Python 3.9 共用）。"""

# 容器把结果以一行 JSON 打印到 stdout，该行以此标记开头；宿主机只解析最后一个标记行。
RESULT_MARKER = "@@URLFIND_RESULT@@"

# 访问结果 outcome 取值
OUTCOME_OK = "ok"              # 页面打开并通过校验，links 为提取的页面链接
OUTCOME_REJECTED = "rejected"  # 页面打开了但校验不通过（404、登录页、验证页等），不重试
OUTCOME_ERROR = "error"        # 浏览器或网络错误，可重试
OUTCOME_TIMEOUT = "timeout"    # 页面加载或整次访问超时，可重试

# 容器内代码路径：宿主机源码根目录 trafficIngestor/ 挂载到 /app
CONTAINER_CODE_PATH = "/app"
CONTAINER_WORKER_SCRIPT = "/app/urlFind/worker/visit_page.py"
