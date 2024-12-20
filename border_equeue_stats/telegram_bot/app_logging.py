"""Contains logging implementation"""
import logging
import os
import sys
from logging.handlers import RotatingFileHandler


def get_all_messages_handler():
    return logging.StreamHandler(sys.stdout)


def get_sys_log_handler():
    os.makedirs('logs', exist_ok=True)
    handler = RotatingFileHandler('logs/sys_logs.log', mode='a', maxBytes=5 * 1024 * 1024,
                                  backupCount=2, encoding=None, delay=False)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    return handler


def set_root_logger():
    all_stream_handler = get_all_messages_handler()
    system_info_logger = get_sys_log_handler()
    logger = logging.getLogger()
    logger.addHandler(all_stream_handler)
    logger.addHandler(system_info_logger)


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
