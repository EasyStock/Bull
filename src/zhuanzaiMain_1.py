import datetime
import schedule
import time
from ColoredLog import StartToInitLogger
from mysql.connect2DB import ConnectToDB
import sys

from zhuanzai.jisilu import CJiSiLu
from categrate import CATEGRAGTE_KE_ZHUAN_ZAI
from zhuanzai.compareIndex import CCompareWithIndex,CZhuanzaiSelect
from DBOperating import GetTradingDateLastN
from zhuanzai.jisilu_selenium import CJiSiLuSelenium

def GetFromJisiluAndWriteToDB(logger):
    logger.info(f'==============begin:{datetime.datetime.utcnow()}==============================')
    dbConnection = ConnectToDB()
    try:
        CJiSiLuSelenium().formatCookie(dbConnection)
    except:
        pass
    jisiLu = CJiSiLu(logger,dbConnection)
    jisiLu.GetFromJisiluAndWriteToDB()
    jisiLu.Categrate(CATEGRAGTE_KE_ZHUAN_ZAI)

    tradingDays = GetTradingDateLastN(dbConnection,3)
    comparer = CCompareWithIndex(dbConnection,logger)
    comparer.CompareWithIndex_ALL(tradingDays)
    logger.info(f'==============end:{datetime.datetime.utcnow()}==============================')

def AutoDownload():
    schedule.every().day.at("19:10").do(GetFromJisiluAndWriteToDB)
    while(True):
        schedule.run_pending()
        time.sleep(1)


def compareWithIndexTest(logger):
    logger.info(f'==============begin:{datetime.datetime.utcnow()}==============================')
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,1000)
    c = CCompareWithIndex(dbConnection,logger)
    c.CompareWithIndex("2023-09-05","2023-10-10")
    #c.CompareWithIndex_ALL(tradingDays)

    select = CZhuanzaiSelect(dbConnection,logger)
    #select.SelectFrom("2023-06-08","2023-06-19")
    select.Select("2023-09-05","2023-10-10",tradingDays)
    logger.info(f'==============end:{datetime.datetime.utcnow()}==============================')


def Test(logger):
    logger.info(f'==============begin:{datetime.datetime.utcnow()}==============================')
    dbConnection = ConnectToDB()
    jisiLu = CJiSiLu(logger,dbConnection)
    jisiLu.request2_login()

if __name__ == "__main__":
    logger = StartToInitLogger("集思录")
    
    #compareWithIndexTest(logger)
    try:
        GetFromJisiluAndWriteToDB(logger)
    except Exception as e:
        print(e)
        sys.exit(1)
