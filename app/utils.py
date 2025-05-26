import os
import logging
from datetime import datetime
import traceback
from logging.handlers import TimedRotatingFileHandler

# Ensure logs directory exists
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)


# Configure logger
def get_logger():
    logger = logging.getLogger("AppLogger")
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        log_file = os.path.join(LOG_DIR, datetime.now().strftime("%Y-%m-%d") + ".log")
        handler = TimedRotatingFileHandler(log_file, when="midnight", backupCount=7)
        handler.suffix = "%Y-%m-%d"
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


# Logging function
def log_message(message, exc: Exception = None):
    logger = get_logger()

    if exc:
        trace = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        logger.error(f"{message}\nException Trace:\n{trace}")
    else:
        logger.info(message)
