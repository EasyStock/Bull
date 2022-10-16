from logging import log
from thsData2.fetchDailyDataFromTHS2 import GetDailyDataMgr
from thsData.constants_10jqka import eng_10jqka_CookieList
import random
from mysql.connect2DB import ConnectToDB
from ColoredLog import StartToInitLogger
import datetime
from fupan.tradingDate import GetTradingDateLastN
from thsData2.fetchNewHighDataFromTHS2 import CFetchNewHighDataFromTHS2
from thsData2.fetchZhangTingDataFromTHS2 import CFetchZhangTingDataFromTHS2
from thsData2.fetchZhangTingLanBanDataFromTHS2 import CFetchZhangTingLanBanDataFromTHS2
from thsData2.fetchDaliangDataFromTHS2 import CFetchDaliangDataFromTHS2
from thsData2.fetchDaliangLanbanDataFromTHS2 import CFetchDaliangLanBanDataFromTHS2
import logging
logger = logging.getLogger()

def GetTHS_V():
    size = len(eng_10jqka_CookieList)
    index = random.randint(0,size-1)
    return eng_10jqka_CookieList[index]


def GetDailyData(dbConnection,date,logger):
    #Validated
    logger.error("======开始获取每日复盘数据=====")
    #v = "A71OYud1gplCtib1xbHCAVHnyhO2WvUS-4xVjH8B-hhBpdNMR6oBfIveZU0M"
    v = GetTHS_V()
    stockBasicInfo_sqls, stockDailyInfo_sql= GetDailyDataMgr(date,v,'stockBasicInfo','stockDailyInfo')
    for index, sql in enumerate(stockBasicInfo_sqls):
        msg = f'''BasicInfo index: {index+1},{sql}''' 
        logger.info(msg)
        dbConnection.Execute(sql)

    for index, sql in enumerate(stockDailyInfo_sql): 
        msg = f'''DailyData index: {index+1},{sql}''' 
        logger.info(msg)
        dbConnection.Execute(sql)

def GetNewHighData(dbConnection,date,logger):
    #Validated
    logger.error("======开始获取新高数据=====")
    v = "A1-cbY4HQBnleEQFv_Aujk746LjsxKa2zQa3CPGS-YtXsHGm-ZRDtt3oR-IC"
    v = GetTHS_V()
    newHigh = CFetchNewHighDataFromTHS2(date,v)
    #newHigh.RequestDailyData()
    newHigh.RequestNewHighData()
    
def GetZhangTingData(dbConnection,date,logger):
    #Validated
    logger.error("======开始获取涨停数据=====")
    v = "A1GippsR1lzvyzo121oWDUULZlfqv9w8b29rSDN6TWE1gH-Iew7VAP-CeIvA"
    zhagngTing = CFetchZhangTingDataFromTHS2(date,v)
    #zhagngTing.RequestDailyData()
    zhagngTing.RequestZhangTingData()
    zhangTingSql = zhagngTing.FormateZhangTingInfoToSQL('stockzhangting') 
    for index,sql in enumerate(zhangTingSql):
        logger.info(f'''index: [{index+1}], {sql}''')
        dbConnection.Execute(sql) 

def oneKeyDailyData(logger):
    logger.info(f'==============begin:{datetime.datetime.now()}==============================')
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,15)
    GetDailyData(dbConnection,tradingDays[-1],logger)
    GetNewHighData(dbConnection,tradingDays[-1],logger)
    GetZhangTingData(dbConnection,tradingDays[-1],logger)
    GetZhangTingLanBanData(dbConnection,tradingDays[-1],logger)
    GetDaLiangData(dbConnection,tradingDays,logger)
    GetDaLiangLanbBanData(dbConnection,tradingDays,logger) 
    logger.info(f'==============end:{datetime.datetime.now()}==============================')

def GetZhangTingLanBanData(dbConnection,date,logger):
    #Validated
    logger.error("======开始获取涨停烂板数据=====")
    v = GetTHS_V()
    zhangTingLanBan = CFetchZhangTingLanBanDataFromTHS2(date,v)
    #zhangTingLanBan.RequestDailyData()
    zhangTingLanBan.RequstZhangTingLanBanData()

def GetDaLiangData(dbConnection,dates,logger):
    #Validated
    logger.error("======开始获取大量数据=====")
    v = GetTHS_V()
    daliang = CFetchDaliangDataFromTHS2(dates,v)
    #daliang.RequestDailyData() 
    daliang.RequestDaliangData() 
    


def GetDaLiangLanbBanData(dbConnection,dates,logger):
    #Validated
    logger.error("======开始获取大量 并且烂板数据=====")
    v = GetTHS_V()
    daliang = CFetchDaliangLanBanDataFromTHS2(dates,v)
    daliang.RequestDaliangLanBanData()
    #daliang.RequestDailyData() 

def Test():
    logger.info(f'==============begin:{datetime.datetime.now()}==============================')
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,15)
    #GetDailyData(dbConnection,tradingDays[-1],logger)
    #GetNewHighData(dbConnection,tradingDays[-1],logger)
    #GetZhangTingData(dbConnection,tradingDays[-1],logger)

    #GetZhangTingLanBanData(dbConnection,tradingDays[-1],logger)
    #GetDaLiangData(dbConnection,tradingDays,logger)
    GetDaLiangLanbBanData(dbConnection,tradingDays,logger) 
    logger.info(f'==============end:{datetime.datetime.now()}==============================')

if __name__ == '__main__':
    logger = StartToInitLogger("同花顺日常数据_new")
    #oneKeyDailyData(logger)
    Test()


