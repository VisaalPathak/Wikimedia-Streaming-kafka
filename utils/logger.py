"""Logging module.

Author: Gaurav Thagunna
Date : 2024-11-08
Description : This is logging module.
"""

import logging
import os
from utils.variables import root_dir, Variables
from datetime import datetime

var = Variables()

log_dir = os.path.join(root_dir, "logs")
if not os.path.isdir(log_dir):
    os.mkdir(log_dir)

filename = datetime.today().strftime("%Y-%m-%d.log")
log_file_path = os.path.join(log_dir, filename)

if var["LOGGING_LEVEL"].lower() == "debug":
    logging_level = logging.DEBUG
elif var["LOGGING_LEVEL"].lower() == "info":
    logging_level = logging.INFO
elif var["LOGGING_LEVEL"].lower() == "error":
    logging_level = logging.ERROR
else:
    logging_level = logging.INFO

date_format = "%Y-%m-%d %H:%M:%S"
logging_format = ("%(asctime)s.%(msecs)03d  %(module)s %(funcName)s %"
                  "(lineno)d %(process)d  %(levelname)-8s %(message)s")

logging.basicConfig(filename=log_file_path, level=logging_level,
                    format=logging_format,
                    datefmt=date_format, filemode='a')

logger = logging
if var["LOGGING_CONSOLE"]:
    logger = logging.getLogger("")
    formatter = logging.Formatter(fmt=logging_format, datefmt=date_format)

    ch = logging.StreamHandler()
    ch.setLevel(logging_level)
    ch.setFormatter(formatter)

    logger.addHandler(ch)
