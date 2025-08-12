import logging
from border_equeue_stats.telegram_bot.app_logging import set_root_logger

set_root_logger()
logging.getLogger("httpx").setLevel(logging.WARNING)
