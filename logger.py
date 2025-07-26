import logging
import logging.config
import os

def setup_logger(log_config_file: str = "./logger.properties"):
    if os.path.exists(log_config_file):
        logging.config.fileConfig(log_config_file)
    else:
        logging.basicConfig(level=logging.INFO)
    return logging.getLogger("db_diff_logger")

