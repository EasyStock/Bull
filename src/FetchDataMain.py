from ColoredLog import StartToInitLogger
from mysql.connect2DB import ConnectToDB
from fupan.tradingDate import GetTradingDateLastN
from kaipanla.kaipanlaDataMgr import RequestIndexData
from iWenCaiMain import GetBanKuaiZhishuDataLastNDays
from thsData.constants_10jqka import eng_10jqka_CookieList

from zhuanzai.jisilu import CJiSiLu
from thsData2.fetchDailyDataFromTHS2 import CFetchDailyDataFromTHS2
from thsData2.fetchNewHighDataFromTHS2 import CFetchNewHighDataFromTHS2
from thsData2.fetchZhangTingDataFromTHS2 import CFetchZhangTingDataFromTHS2
from thsData2.fetchZhangTingLanBanDataFromTHS2 import CFetchZhangTingLanBanDataFromTHS2
from thsData2.fetchDaliangDataFromTHS2 import CFetchDaliangDataFromTHS2
from thsData2.fetchDaliangLanbanDataFromTHS2 import CFetchDaliangLanBanDataFromTHS2
from thsData.fetchNoZhangTingLanBanData import CFetchNoZhangTingData
from eastmoney.dragon import CDragonFetcher

import random
import datetime
import pytz

def _GetTHS_V():
    size = len(eng_10jqka_CookieList)
    index = random.randint(0,size-1)
    return eng_10jqka_CookieList[index]

def _GetDailyDataEx(dbConnection,date,logger):
    # 从同花顺上获取所有股票的基础数据和成交数据
    logger.info("\n\n======开始同花顺上获取每日复盘数据=====")
    v = _GetTHS_V()
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

def _GetNewHighData(dbConnection,date,logger):
     # 从同花顺上获取创新高的数据
    logger.error("\n\n======开始获取新高数据=====")
    v = _GetTHS_V()
    newHigh = CFetchNewHighDataFromTHS2(date,v)
    newHigh.RequestNewHighDataEX()

def _GetZhangTingData(dbConnection,date,logger):
    logger.error("\n\n======开始获取涨停数据=====")
    v = _GetTHS_V()
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

def _GetZhangTingLanBanData(dbConnection,date,logger):
    logger.error("\n\n======开始获取涨停烂板数据=====")
    v = _GetTHS_V()
    zhangTingLanBan = CFetchZhangTingLanBanDataFromTHS2(date,v)
    zhangTingLanBan.RequstZhangTingLanBanDataEX()

def _GetDaLiangData(dbConnection,dates,logger):
    logger.error("\n\n======开始获取大量数据=====")
    v = _GetTHS_V()
    daliang = CFetchDaliangDataFromTHS2(dates,v)
    daliang.RequestDaliangDataEX() 

def _GetDaLiangLanbBanData(dbConnection,dates,logger):
    logger.error("\n\n======开始获取大量 并且烂板数据=====")
    v = _GetTHS_V()
    daliang = CFetchDaliangLanBanDataFromTHS2(dates,v)
    daliang.RequestDaliangLanBanDataEX()

def _GetNoZhangTingLanBanData(dbConnection,date,logger):
    logger.error("\n\n======开始获取烂长上影线数据=====")
    #上影线数据
    dailyFetcher = CFetchNoZhangTingData(dbConnection,date)
    dailyFetcher.FetchNoZhangTingData()

#####################################以下是入口函数######################################################
def GetDataFromJisiluAndWriteToDB(dbConnection,logger):
    logger.info(f'==============开始从集思录上获取数据:{datetime.datetime.utcnow()}==============================')
    jisiLu = CJiSiLu(logger,dbConnection)
    jisiLu.GetFromJisiluAndWriteToDB()
    jisiLu.GetNewStockCalendar()
    logger.info(f'==============结束从集思录上获取数据:{datetime.datetime.utcnow()}==============================\n')

def GetDataFromTHSAndWriteToDB(dbConnection,tradingDays,logger):
    logger.info(f'==============开始从同花顺上获取数据:{datetime.datetime.utcnow()}==============================')
    _GetDailyDataEx(dbConnection,tradingDays[-1],logger)
    _GetNewHighData(dbConnection,tradingDays[-1],logger)
    _GetZhangTingData(dbConnection,tradingDays[-1],logger)

    _GetZhangTingLanBanData(dbConnection,tradingDays[-1],logger)
    _GetDaLiangData(dbConnection,tradingDays,logger)

    _GetDaLiangLanbBanData(dbConnection,tradingDays,logger) 
    _GetNoZhangTingLanBanData(dbConnection,tradingDays[-1],logger)
    GetBanKuaiZhishuDataLastNDays(dbConnection,tradingDays[-3:],logger)
    logger.info(f'==============结束从同花顺上获取数据:{datetime.datetime.utcnow()}==============================\n')

def GetDataFromKaiPanLaAndWriteToDB(dbConnection,tradingDays,logger):
    logger.info(f'==============开始从开盘啦上获取数据:{datetime.datetime.utcnow()}==============================')
    lasN = 3
    if len(tradingDays)>lasN:
        tradingDays = tradingDays[-lasN:]
    RequestIndexData(tradingDays,dbConnection)
    logger.info(f'==============结束从开盘啦上获取数据:{datetime.datetime.utcnow()}==============================\n')

def GetDataFromKEastMonenyAndWriteToDB(dbConnection,tradingDays,logger):
    logger.info(f'==============开始从东方财富网上获取数据:{datetime.datetime.utcnow()}==============================')
    #获取龙虎榜数据
    tableName = "`stock`.`dragon`"
    date = tradingDays[-1]
    f = CDragonFetcher()
    f.FetchDailyDataAndToDB(date,tableName,dbConnection)

    logger.info(f'==============结束从东方财富网上获取数据:{datetime.datetime.utcnow()}==============================\n')

#####################################入口函数######################################################
    
if __name__ == "__main__":
    logger = StartToInitLogger("获取数据入口")
    dbConnection = ConnectToDB()
    lastN = 15
    tradingDays = GetTradingDateLastN(dbConnection,lastN)
    now = datetime.datetime.now(pytz.timezone('Asia/Shanghai'))
    logger.error(f"现在是北京时间是:{now}")

    import argparse
    parser = argparse.ArgumentParser()
    helpStr = f'''
    -o : options
    '''

    parser.add_argument('-o','--options', action="store",default=True,nargs="+",type=int, help="0. 获取全部数据, 1. 从同花顺上获取数据 2. 从集思录上获取数据 3. 从开盘啦上获取数据 4. 从东方财富网上获取数据")
    args = parser.parse_args()
    #args.options = [0,1,2,3,4]
    if 0 in args.options:
        ######################
        #从同花顺上获取数据
        GetDataFromTHSAndWriteToDB(dbConnection,tradingDays,logger)

        #从集思录上获取数据
        GetDataFromJisiluAndWriteToDB(dbConnection,logger)

        #从开盘啦上获取数据
        GetDataFromKaiPanLaAndWriteToDB(dbConnection,tradingDays,logger)

        #从东方财富网上获取数据
        GetDataFromKEastMonenyAndWriteToDB(dbConnection,tradingDays,logger)
        ###################### 
    else:

        if 1 in args.options:
            #从同花顺上获取数据
            GetDataFromTHSAndWriteToDB(dbConnection,tradingDays,logger)

        if 2 in args.options:
            #从集思录上获取数据
            GetDataFromJisiluAndWriteToDB(dbConnection,logger)

        if 3 in args.options:
            #从开盘啦上获取数据
            GetDataFromKaiPanLaAndWriteToDB(dbConnection,tradingDays,logger)

        if 4 in args.options:
            #从东方财富网上获取数据
            GetDataFromKEastMonenyAndWriteToDB(dbConnection,tradingDays,logger)

