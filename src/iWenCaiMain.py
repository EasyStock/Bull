from iWenCai.iWenCaiApi import CIWenCaiAPI
from mysql.connect2DB import ConnectToDB
from iWenCai.FetchBanKuaiData import CFetchBanKuaiData
from iWenCai.FetchStockDailyData import CFetchStockDailyData
from DBOperating import GetTradingDateLastN
from ColoredLog import StartToInitLogger
from iWenCai.FetchKeZhuanZaiDailyData import CFetchKeZhuanZaiDailyData
from iWenCai.FetchVMAData import CFetchVMAData
from iWenCai.FetchIndex import CFetchIndexDataMgr
from iWenCai.FetchBanKuaiStockMatch import CFetchBanKuaiStockMatchData
from bankuai.bankuaiMgr import GetAllBasicBanKuaiData
from iWenCai.FetchZhaBanData import CFetchZhaBanDailyData
import time

def GetBanKuaiZhishuData(dbConnection,tradingDays):
    lastDay = tradingDays[-1]
    f = CFetchBanKuaiData(dbConnection,lastDay)
    f.RequestAllPagesDataAndWriteToDB(100)

def GetBanKuaiZhishuDataLastNDays(dbConnection,tradingDays,logger):
    for treadingDay in tradingDays:
        logger.info(f"============================开始获取板块指数{treadingDay}====================================")
        f = CFetchBanKuaiData(dbConnection,treadingDay)
        f.RequestAllPagesDataAndWriteToDB(100)

def GetVMAData(dbConnection,tradingDays,logger):
    #只能获取当天的数据
    lastDay = tradingDays[-1]
    logger.info(f"============================开始获取成交量/成交量均量（V/MA）数据{lastDay}====================================")
    f = CFetchVMAData(dbConnection,lastDay)
    f.RequestAllPagesDataAndWriteToDB(100)

def TestGetDailyData(dbConnection):
    lastDay = tradingDays[-2]
    lastDay = "2023-12-10"
    f = CFetchStockDailyData(dbConnection,lastDay)
    f.RequestAllPagesDataAndWriteToDB(100)

    # api = CIWenCaiAPI(dbConnection = dbConnection)
    # payload = {
	# "source": "Ths_iwencai_Xuangu",
	# "version": "2.0",
	# "query_area": "",
	# "block_list": "",
	# "add_info": "{\"urp\":{\"scene\":1,\"company\":1,\"business\":1},\"contentType\":\"json\",\"searchInfo\":true}",
	# "question": "2023.12.21 前复权开盘价，前复权收盘价，前复权最高价，前复权最低价，前复权涨跌幅, 成交量，成交额，上市天数,所属概念",
	# "perpage": "10",
	# "page": 1,
	# "secondary_intent": "stock",
	# "log_info": "{\"input_type\":\"typewrite\"}",
	# "rsh": "240679370"  
    # }
    # api.RequestAllPagesData(payload= payload,perPage=100)

def TestFechVMAData(dbConnection,tradingDays):
    lastDay = tradingDays[-1]
    f = CFetchVMAData(dbConnection,lastDay)
    f.RequestAllPagesDataAndWriteToDB(100)

def GetKeZhuanZaiDailyDataLastNDays(dbConnection,tradingDays,logger):
    for treadingDay in tradingDays:
        logger.info(f"============================开始获取可转债数据: {treadingDay}====================================")
        f = CFetchKeZhuanZaiDailyData(dbConnection,treadingDay)
        f.RequestAllPagesDataAndWriteToDB(100)

def GetIndexData(dbConnection,tradingDays):
    lastDay = tradingDays[-1]
    f = CFetchIndexDataMgr(dbConnection,lastDay)
    f.RequestAllPagesDataAndWriteToDB()

def GetIndexDataLastNDays(dbConnection,tradingDays,logger):
    for treadingDay in tradingDays:
        logger.info(f"============================开始获取指数数据: {treadingDay}====================================")
        f = CFetchIndexDataMgr(dbConnection,treadingDay)
        f.RequestAllPagesDataAndWriteToDB()

def GetBanKuaiStockMatchData(dbConnection,tradingDays):
    lastDay = tradingDays[-1]
    bankuai = GetAllBasicBanKuaiData(dbConnection)
    for _,row in bankuai.iterrows():
        f = CFetchBanKuaiStockMatchData(dbConnection,lastDay,row["板块代码"],row["板块名称"])
        f.RequestAllPagesDataAndWriteToDB(100)
        time.sleep(2)

def GetZhaBanData(dbConnection,tradingDays):
    lastDay = tradingDays[-1]
    f = CFetchZhaBanDailyData(dbConnection,lastDay)
    f.RequestAllPagesDataAndWriteToDB()

def GetZhaBanDataLastNDays(dbConnection,tradingDays,logger):
    for treadingDay in tradingDays:
        logger.info(f"============================开始获取炸板数据: {treadingDay}====================================")
        f = CFetchZhaBanDailyData(dbConnection,treadingDay)
        f.RequestAllPagesDataAndWriteToDB()

if __name__ == '__main__':
    logger = StartToInitLogger("爱问财获取数据")
    dbConnection = ConnectToDB()
    lastN = 3
    tradingDays = GetTradingDateLastN(dbConnection,lastN)

    #GetBanKuaiZhishuData(dbConnection,tradingDays)
    #GetBanKuaiZhishuDataLastNDays(dbConnection,lastN)
    #TestGetDailyData(dbConnection)
    #GetKeZhuanZaiDailyDataLastNDays(dbConnection,tradingDays,logger)
    #TestFechVMAData(dbConnection,tradingDays)
    #GetIndexDataLastNDays(dbConnection,tradingDays,logger)
    GetVMAData(dbConnection,tradingDays,logger)

    #GetZhaBanDataLastNDays(dbConnection,tradingDays,logger)
