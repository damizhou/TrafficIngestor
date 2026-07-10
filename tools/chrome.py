"""Standard Chrome driver facade."""

import os
import sys

_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from tools.base_chrome import BaseChromeDriverFactory


class ChromeDriverFactory(BaseChromeDriverFactory):
    """Create the standard Chrome capture variant."""


_FACTORY = ChromeDriverFactory()


def create_chrome_driver(task_name=None, formatted_time=None, parsers=None,
                         enable_ssl_key_log=True, data_base_dir=None,
                         proxy_server=None, proxy_bypass_list=None, logger=None,
                         blocked_hosts=None, chrome_binary_path=None,
                         chromedriver_path=None, artifact_label=None):
    return _FACTORY.create_driver(
        task_name=task_name,
        formatted_time=formatted_time,
        parsers=parsers,
        enable_ssl_key_log=enable_ssl_key_log,
        data_base_dir=data_base_dir,
        proxy_server=proxy_server,
        proxy_bypass_list=proxy_bypass_list,
        logger=logger,
        blocked_hosts=blocked_hosts,
        chrome_binary_path=chrome_binary_path,
        chromedriver_path=chromedriver_path,
        artifact_label=artifact_label,
    )
