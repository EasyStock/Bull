from iWenCai.iWenCaiApi import CIWenCaiAPI
from mysql.connect2DB import ConnectToDB
from iWenCai.FetchBanKuaiData import CFetchBanKuaiData
from iWenCai.FetchStockDailyData import CFetchStockDailyData
from DBOperating import GetTradingDateLastN
from ColoredLog import StartToInitLogger

def GetBanKuaiZhishuData(dbConnection,tradingDays):
    lastDay = tradingDays[-1]
    f = CFetchBanKuaiData(dbConnection,lastDay)
    f.RequestAllPagesDataAndWriteToDB(100)

def GetBanKuaiZhishuDataLastNDays(dbConnection,tradingDays,logger):
    for treadingDay in tradingDays:
        logger.info(f"============================开始获取板块指数{treadingDay}====================================")
        f = CFetchBanKuaiData(dbConnection,treadingDay)
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


if __name__ == '__main__':
    logger = StartToInitLogger("爱问财获取数据")
    dbConnection = ConnectToDB()
    lastN = 1
    tradingDays = GetTradingDateLastN(dbConnection,lastN)

    #GetBanKuaiZhishuData(dbConnection,tradingDays)
    GetBanKuaiZhishuDataLastNDays(dbConnection,lastN)
    #TestGetDailyData(dbConnection)
