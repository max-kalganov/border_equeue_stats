"""Contains logging implementation"""
import logging
import os
import sys
from logging.handlers import RotatingFileHandler

APP_MAIN_LOGGER_NAME = 'app_main'
STAT_INTERFACE_LOGGER_NAME = 'stats_interface'
APP_PUSH_NOTIFICATIONS_LOGGER_NAME = 'app_push_notification'

ALL_MAIN_LOGGER_NAMES = {APP_MAIN_LOGGER_NAME, STAT_INTERFACE_LOGGER_NAME, APP_PUSH_NOTIFICATIONS_LOGGER_NAME}


class AppMainLoggersFilter(logging.Filter):
    def filter(self, record):
        return record.name in ALL_MAIN_LOGGER_NAMES


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


def get_user_logger(user_id, chat_id):
    users_logs_dir = os.path.join('logs', 'users_logs')
    os.makedirs(users_logs_dir, exist_ok=True)
    handler = RotatingFileHandler(os.path.join(users_logs_dir, f"{hash(str(user_id) + str(chat_id))}.log"),
                                  mode='a', maxBytes=5 * 1024 * 1024,
                                  backupCount=2, encoding=None, delay=False)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger = logging.getLogger('user_log')
    logger.addHandler(handler)
    return logger
