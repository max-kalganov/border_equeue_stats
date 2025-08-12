import logging
from border_equeue_stats.telegram_bot.logging_utils import set_root_logger

set_root_logger()
logging.getLogger("httpx").setLevel(logging.WARNING)
