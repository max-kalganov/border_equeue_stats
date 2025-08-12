"""Contains logging implementation"""
import os
import sys
import logging
import hashlib
from logging.handlers import RotatingFileHandler

from border_equeue_stats.constants import COMMON_LOGGERS


class UserLogger(logging.Logger):
  def __init__(self, user_id, level=logging.NOTSET):
    name = 'user_logger'
    super().__init__(name, level)
    self.extra_info = {'user_id': user_id}
    user_id_hash = hashlib.md5(str(user_id).encode()).hexdigest()
    print(self.extra_info, user_id_hash)

    users_logs_dir = os.path.join('logs', 'users_logs')
    os.makedirs(users_logs_dir, exist_ok=True)
    handler = RotatingFileHandler(os.path.join(users_logs_dir, f"{user_id_hash}.log"),
                                  mode='a', maxBytes=5 * 1024 * 1024,
                                  backupCount=2, encoding=None, delay=False)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    self.addHandler(handler)
    

class AppMainLoggersFilter(logging.Filter):
    def filter(self, record):
        return record.name in COMMON_LOGGERS


def get_all_messages_handler():
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    return handler


def get_sys_log_handler():
    os.makedirs('logs', exist_ok=True)
    handler = RotatingFileHandler('logs/sys_logs.log', mode='a', maxBytes=5 * 1024 * 1024,
                                  backupCount=2, encoding=None, delay=False)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    handler.addFilter(AppMainLoggersFilter())
    return handler


def set_root_logger():
    all_stream_handler = get_all_messages_handler()
    system_info_logger = get_sys_log_handler()
    logger = logging.getLogger()
    logger.addHandler(all_stream_handler)
    logger.addHandler(system_info_logger)
    logger.setLevel(logging.INFO)
