from tushareData.tushareDaily import fethTushareDailyData
from mysql.connect2DB import ConnectToDB,DataFrameToSqls_REPLACE
import datetime
from ColoredLog import StartToInitLogger
import logging
import schedule
import time

logger = logging.getLogger()

def FetchTushareDailyDailyDataLastN(stockID, lastN):
    tushare = fethTushareDailyData()
    df = tushare.FetchDailyDataLastN(stockID,lastN)
    return df

def fetchAllStockIDs(dbConnection):
    sql = "select `股票代码` from `stockBasicInfo`;"
    results,_ = dbConnection.Query(sql)
    stockIDs = [result[0] for result in results]
    return stockIDs

def FetchAllInOne():
    dbConnection = ConnectToDB()
    stockIDs = fetchAllStockIDs(dbConnection)
    lastN=30
    tushareTableName = 'stockDailyInfo_Tushare'
    count = len(stockIDs)
    for index, stockID in enumerate(stockIDs):
        logger.info(f'index: {index+1:05d}, total: {count}, fetch {stockID} begin!')
        df = FetchTushareDailyDailyDataLastN(stockID,lastN)
        sqls = DataFrameToSqls_REPLACE(df,tushareTableName)
        for sql in sqls:
            logger.info(sql)
            dbConnection.Execute(sql)
    
def UpdateTradingDate():
    dbConnection = ConnectToDB()
    tushare = fethTushareDailyData()
    df = tushare.FetchTreadingDate('20220101','20251231')
    sqls = DataFrameToSqls_REPLACE(df,'treadingDay')
    for sql in sqls:
        dbConnection.Execute(sql)
        logger.info(sql)
    
def Test():
    tushare = fethTushareDailyData()
    df = tushare.FetchDailyDataLastN("000001.SZ",750)
    logger.info(str(df))
    
def tushareFun():
    logger.info(f'==============begin:{datetime.datetime.utcnow()}==============================')
    StartToInitLogger("TuShare日常数据")
    UpdateTradingDate()
    FetchAllInOne()
    logger.info(f'==============end:{datetime.datetime.utcnow()}==============================')

def AutoDownload():
    schedule.every().day.at("18:07").do(tushareFun)
    while(True):
        schedule.run_pending()
        time.sleep(1)
        
        
if __name__ == "__main__":
    #StartToInitLogger()
    # UpdateTradingDate()
    #AutoDownload()
    tushareFun()
    #Test()