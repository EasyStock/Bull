import datetime
import schedule
import time
from ColoredLog import StartToInitLogger
from mysql.connect2DB import ConnectToDB
import sys

from zhuanzai.jisilu import CJiSiLu
from categrate import CATEGRAGTE_KE_ZHUAN_ZAI

def GetFromJisiluAndWriteToDB(logger):
    logger.info(f'==============begin:{datetime.datetime.now()}==============================')
    dbConnection = ConnectToDB()
    jisiLu = CJiSiLu(logger,dbConnection)
    jisiLu.GetFromJisiluAndWriteToDB()
    jisiLu.Categrate(CATEGRAGTE_KE_ZHUAN_ZAI)
    logger.info(f'==============end:{datetime.datetime.now()}==============================')

def AutoDownload():
    schedule.every().day.at("19:10").do(GetFromJisiluAndWriteToDB)
    while(True):
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    logger = StartToInitLogger("集思录")
    try:
        GetFromJisiluAndWriteToDB(logger)
    except Exception as e:
        print(e)
        sys.exit(1)
