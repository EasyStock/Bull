from thsData2.fetchDailyDataFromTHS2 import CFetchDailyDataFromTHS2
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
import sys

import logging
from thsData.fetchNoZhangTingLanBanData import CFetchNoZhangTingData

import traceback
logger = logging.getLogger()

def GetTHS_V():
    size = len(eng_10jqka_CookieList)
    index = random.randint(0,size-1)
    return eng_10jqka_CookieList[index]


def GetDailyDataEx(dbConnection,date,logger):
    #Validated
    logger.error("\n\n======开始获取每日复盘数据=====")
    #v = "A71OYud1gplCtib1xbHCAVHnyhO2WvUS-4xVjH8B-hhBpdNMR6oBfIveZU0M"
    v = GetTHS_V()
    daily = CFetchDailyDataFromTHS2(date,v)
    daily.RequestDailyData_MultiPages()

    stockBasicInfo_sqls = daily.FormateBacicInfoToSQL('stockBasicInfo')
    stockDailyInfo_sqls = daily.FormateDailyInfoToSQL('stockDailyInfo')
    printcount = 1500
    for index, sql in enumerate(stockBasicInfo_sqls):
        msg = f'''BasicInfo index: [{index+1:^5d}],{sql}'''
        if index %printcount == 0:
            logger.info(msg)
        
        if index %(len(stockBasicInfo_sqls)-1) == 0 and index != 0:
            logger.warning(msg)

        dbConnection.Execute(sql)

    for index, sql in enumerate(stockDailyInfo_sqls): 
        msg = f'''DailyData index: [{index+1:^5d}],{sql}'''
        if index %printcount == 0:
            logger.info(msg) 

        if index %(len(stockDailyInfo_sqls)-1) == 0 and index != 0:
            logger.warning(msg)

        dbConnection.Execute(sql)

def GetNewHighData(dbConnection,date,logger):
    #Validated
    logger.error("\n\n======开始获取新高数据=====")
    v = "A1-cbY4HQBnleEQFv_Aujk746LjsxKa2zQa3CPGS-YtXsHGm-ZRDtt3oR-IC"
    v = GetTHS_V()
    newHigh = CFetchNewHighDataFromTHS2(date,v)
    newHigh.RequestNewHighDataEX()


def GetZhangTingData(dbConnection,date,logger):
    #Validated
    logger.error("\n\n======开始获取涨停数据=====")
    v = "A1GippsR1lzvyzo121oWDUULZlfqv9w8b29rSDN6TWE1gH-Iew7VAP-CeIvA"
    zhagngTing = CFetchZhangTingDataFromTHS2(date,v)
    zhagngTing.RequestZhangTingDataEX()
    zhangTingSql = zhagngTing.FormateZhangTingInfoToSQL('stockzhangting')
    printcount = 50 
    for index,sql in enumerate(zhangTingSql):
        msg = f'''index: [{index+1}], {sql}'''
        if index %printcount == 0:
            logger.info(msg)
        
        if index %(len(zhangTingSql)-1) == 0 and index != 0:
            logger.warning(msg)

        dbConnection.Execute(sql) 

def GetNoZhangTingLanBanData(dbConnection,date,logger):
    logger.error("开始获取烂长上影线数据")
    #上影线数据
    dailyFetcher = CFetchNoZhangTingData(dbConnection,date)
    dailyFetcher.FetchNoZhangTingData()

def oneKeyDailyData(logger):
    logger.info(f'==============begin:{datetime.datetime.utcnow()}==============================')
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,15)
    GetDailyDataEx(dbConnection,tradingDays[-1],logger)
    GetNewHighData(dbConnection,tradingDays[-1],logger)

    GetZhangTingData(dbConnection,tradingDays[-1],logger)
    GetZhangTingLanBanData(dbConnection,tradingDays[-1],logger)
    GetDaLiangData(dbConnection,tradingDays,logger)
    GetDaLiangLanbBanData(dbConnection,tradingDays,logger) 
    GetNoZhangTingLanBanData(dbConnection,tradingDays[-1],logger)
    logger.info(f'==============end:{datetime.datetime.utcnow()}==============================')

def GetZhangTingLanBanData(dbConnection,date,logger):
    #Validated
    logger.error("\n\n======开始获取涨停烂板数据=====")
    v = GetTHS_V()
    zhangTingLanBan = CFetchZhangTingLanBanDataFromTHS2(date,v)
    zhangTingLanBan.RequstZhangTingLanBanDataEX()

    
def GetDaLiangData(dbConnection,dates,logger):
    #Validated
    logger.error("\n\n======开始获取大量数据=====")
    v = GetTHS_V()
    daliang = CFetchDaliangDataFromTHS2(dates,v)
    daliang.RequestDaliangDataEX() 

def GetDaLiangLanbBanData(dbConnection,dates,logger):
    #Validated
    logger.error("\n\n======开始获取大量 并且烂板数据=====")
    v = GetTHS_V()
    daliang = CFetchDaliangLanBanDataFromTHS2(dates,v)
    daliang.RequestDaliangLanBanDataEX()


def Test():
    logger.info(f'==============begin:{datetime.datetime.utcnow()}==============================')
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,15)

    GetDailyDataEx(dbConnection,tradingDays[-1],logger)
    GetNewHighData(dbConnection,tradingDays[-1],logger)
    GetZhangTingData(dbConnection,tradingDays[-1],logger)
    GetZhangTingLanBanData(dbConnection,tradingDays[-1],logger)
    GetDaLiangData(dbConnection,tradingDays,logger)
    GetDaLiangLanbBanData(dbConnection,tradingDays,logger) 
    logger.info(f'==============end:{datetime.datetime.utcnow()}==============================')

if __name__ == '__main__':
    logger = StartToInitLogger("同花顺日常数据_new")
    oneKeyDailyData(logger)


