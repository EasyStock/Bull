#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   ColoredLog.py
@Time    :   2020/12/02 21:05:33
@Author  :   JianPing Huang 
@Contact :   yuchonghuang@126.com
'''

import logging
import datetime
import os
import pytz

BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)
#The background is set with 40 plus the number of the color, and the foreground with 30

#These are the sequences need to get colored ouput
RESET_SEQ = "\033[0m"
COLOR_SEQ = "\033[1;%dm"
BOLD_SEQ = "\033[1m"

def formatter_message(message, use_color = True):
    if use_color:
        message = message.replace("$RESET", RESET_SEQ).replace("$BOLD", BOLD_SEQ)
    else:
        message = message.replace("$RESET", "").replace("$BOLD", "")
    return message


COLORS = {
    'WARNING': YELLOW,
    'INFO': GREEN,
    'DEBUG': BLUE,
    'CRITICAL': YELLOW,
    'ERROR': RED
}


class ColoredFormatter(logging.Formatter):
    def __init__(self, msg, use_color = True):
        logging.Formatter.__init__(self, msg)
        self.use_color = use_color

    def format(self, record):
        levelname = record.levelname
        msg = record.msg
        if self.use_color and levelname in COLORS:
            levelname_color = COLOR_SEQ % (30 + COLORS[levelname]) + levelname + RESET_SEQ
            record.levelname = levelname_color

            msg_color = COLOR_SEQ % (30 + COLORS[levelname]) + msg + RESET_SEQ
            record.msg = msg_color
        return logging.Formatter.format(self, record)


def InitLogger(fileName):
    logger = logging.getLogger()
    logger.setLevel(level = logging.INFO)
    handler = logging.FileHandler(fileName)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - [%(levelname)-8s] - %(message)s (%(filename)s %(lineno)d)')
    handler.setFormatter(formatter)

    FORMAT = "[$BOLD%(asctime)-20s$RESET][%(levelname)-6s]  %(message)s"
    COLOR_FORMAT = formatter_message(FORMAT, True)
    color_formatter = ColoredFormatter(COLOR_FORMAT)
    console = logging.StreamHandler()
    console.setFormatter(color_formatter)
    console.setLevel(logging.DEBUG)
    
    logger.addHandler(handler)
    logger.addHandler(console)
    return logger



def StartToInitLogger(title=None):
    folder = '/tmp/'
    now = datetime.datetime.now(pytz.timezone('Asia/Shanghai')).strftime("%Y%m%d%H%M%S")
    fullPath = os.path.join(folder,f'{now}_{title}.txt')
    logger = InitLogger(fullPath)
    return logger