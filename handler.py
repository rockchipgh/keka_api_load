# Import the necessary modules
import logging
import os

# Define a function to clean up log files if they exceed a certain size
def clean_dump():
    try:
        statinfo = os.stat('sql.log')
        if statinfo.st_size>10_485_760:
            os.remove("sql.log")
    except FileNotFoundError:
        pass

    try:
        statinfo = os.stat('complete.log')
        if statinfo.st_size>52_428_800:
            os.remove("complete.log")
    except FileNotFoundError:
        pass

# Define a function to set up a logger for logging to a file
def setup_logger(name, log_file, level=logging.INFO):
    """To setup as many loggers as you want"""
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

    handler = logging.FileHandler(log_file)        
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger
