from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import os


def create_chrome_driver():
    # 在当前目录中创建download文件夹
    download_folder = os.path.join(os.getcwd(), 'download')
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)
    # 创建 ChromeOptions 实例
    chrome_options = Options()
    os.environ["SE_OFFLINE"] = "true"
    _ACCEPT_LANGUAGE = "zh-CN,zh;q=0.9"
    _LANG_PRIMARY = "zh-CN"
    chrome_options.binary_location = "/usr/bin/google-chrome"  # 固定 Chrome 路径，避免联网查询
    chrome_options.add_argument('--headless')  # 无界面模式
    chrome_options.add_argument("--disable-gpu")  # 禁用 GPU 加速
    chrome_options.add_argument("--no-sandbox")  # 禁用沙盒
    chrome_options.add_argument("--disable-dev-shm-usage")  # 限制使用/dev/shm
    chrome_options.add_argument("--incognito")  # 隐身模式
    chrome_options.add_argument("--disable-application-cache")  # 禁用应用缓存
    chrome_options.add_argument("--disable-extensions")  # 禁用扩展
    chrome_options.add_argument("--disable-infobars")  # 禁用信息栏
    chrome_options.add_argument("--disable-software-rasterizer")  # 禁用软件光栅化
    chrome_options.add_argument("--autoplay-policy=no-user-gesture-required")  # 允许自动播放
    chrome_options.add_argument(f"--lang={_LANG_PRIMARY}") # ✅ 启动语言
    chrome_options.add_argument("--disable-background-networking")  # 降低背景“噪音”联网
    chrome_options.add_argument("--no-first-run")
    chrome_options.add_argument("--no-default-browser-check")
    chrome_options.add_argument("--homepage=about:blank")
    chrome_options.add_argument("--log-net-log=/tmp/netlog.json")
    chrome_options.add_argument("--net-log-capture-mode=Everything")

    # 设置实验性首选项
    prefs = {
        "profile.default_content_settings.popups": 0,
        "credentials_enable_service": False,  # 禁用密码管理器弹窗
        "profile.password_manager_enabled": False,  # 禁用密码管理器
        "download.default_directory": download_folder,  # 默认下载目录
        "download.prompt_for_download": False,  # 不提示下载
        "download.directory_upgrade": True,  # 升级下载目录
        "safebrowsing.enabled": True,  # 启用安全浏览
        "intl.accept_languages": _ACCEPT_LANGUAGE,  # ✅ 首选语言
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # 启用性能日志记录
    chrome_options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    # 创建 WebDriver 实例
    service = Service(executable_path="/usr/local/bin/chromedriver")
    browser = webdriver.Chrome(service=service, options=chrome_options)
    browser.execute_cdp_cmd('Network.enable', {})
    browser.execute_cdp_cmd('Network.setExtraHTTPHeaders', {'headers': {'Accept-Language': _ACCEPT_LANGUAGE}})
    browser.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument',
                            {'source': '''
                            Object.defineProperty(navigator,"webdriver",{get:()=>undefined});
                            Object.defineProperty(navigator,"language",{get:()=> "zh-CN"});
                            Object.defineProperty(navigator,"languages",{get:()=> ["zh-CN","zh"]});
                            '''.strip()})
    return browser

