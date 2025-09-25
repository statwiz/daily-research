import os
import logging
import sys
from logging.handlers import TimedRotatingFileHandler

class ColorFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': '\033[36m',
        'INFO': '\033[32m',
        'WARNING': '\033[33m',
        'ERROR': '\033[31m',
        'CRITICAL': '\033[41m'
    }
    RESET = '\033[0m'
    WHITE = '\033[37m'

    def format(self, record: logging.LogRecord) -> str:
        original_levelname = record.levelname
        color = self.COLORS.get(original_levelname)
        if color:
            record.levelname = f"{color}{original_levelname}{self.RESET}{self.WHITE}"
        try:
            formatted = super().format(record)
        finally:
            record.levelname = original_levelname
        return f"{self.WHITE}{formatted}{self.RESET}"


class MaxLevelFilter(logging.Filter):
    def __init__(self, max_level: int):
        super().__init__()
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno < self.max_level


def get_logger(name: str = "xxx",
               log_dir: str = "logs",
               filename: str = "xxx.log",
               level: int = logging.INFO,
               when: str = "midnight",
               backup_count: int = 30) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False
    if logger.handlers:
        return logger

    # 确保日志目录相对于脚本所在目录（项目根目录）
    script_dir = os.path.dirname(os.path.abspath(__file__))
    full_log_dir = os.path.join(script_dir, log_dir)
    os.makedirs(full_log_dir, exist_ok=True)

    file_handler = TimedRotatingFileHandler(
        os.path.join(full_log_dir, filename), when=when, backupCount=backup_count, encoding='utf-8'
    )
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    ))

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.addFilter(MaxLevelFilter(logging.ERROR))
    stdout_handler.setFormatter(ColorFormatter(
        "%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    ))

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.ERROR)
    stderr_handler.setFormatter(ColorFormatter(
        "%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    ))

    logger.addHandler(file_handler)
    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)
    return logger 