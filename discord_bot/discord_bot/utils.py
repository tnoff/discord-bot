import logging
from logging.handlers import RotatingFileHandler

def get_logger(logger_name, log_file):
    logger = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    logger.setLevel(logging.DEBUG)
    fh = RotatingFileHandler(log_file,
                             backupCount=2,
                             maxBytes=((2 ** 20) * 10))
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger
