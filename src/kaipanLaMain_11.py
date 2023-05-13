from kaipanla.kaipanlaDataMgr import RequestVolumnDataByDates,RequestZhangTingDataByDates,RequestZhaBanDataByDates,RequestDieTingDataByDates,RequestZhiRanZhangTingDataByDates
from mysql.connect2DB import ConnectToDB
from fupan.tradingDate import GetTradingDateLastN
import urllib3
urllib3.disable_warnings()

def RequestKaiPanLaVolumnData(dbConnection,tradingDays,lastN = 3):
    RequestVolumnDataByDates(tradingDays,dbConnection)

def RequestKaiPanLaZhangTingData(dbConnection,tradingDays,lastN = 3):
    RequestZhangTingDataByDates(tradingDays,dbConnection)


def RequestKaiPanLaZhaBanData(dbConnection,tradingDays,lastN = 3):
    RequestZhaBanDataByDates(tradingDays,dbConnection)


def RequestKaiPanLaDieTingData(dbConnection,tradingDays,lastN = 3):
    RequestDieTingDataByDates(tradingDays,dbConnection)


def RequestKaiPanLaZhiRanZhangTingData(dbConnection,tradingDays,lastN = 3):
    RequestZhiRanZhangTingDataByDates(tradingDays,dbConnection)

def MergeDataTo(dbConnection,tradingDays):
    lastDay = tradingDays[-1]
    sql1  = f'''select volumn,delta,ratio from stock.kaipanla_volumn where date = "{lastDay}";'''
    result1,_ = dbConnection.Query(sql1)

    sql2 = f'''SELECT count(*) FROM stock.kaipanla_zhangting where date = '{lastDay}';'''
    result2,_ = dbConnection.Query(sql2)

    sql3 = f'''SELECT count(*) FROM stock.kaipanla_zhaban where date = '{lastDay}';'''
    result3,_ = dbConnection.Query(sql3)
    
    sql4 = f'''SELECT count(*) FROM stock.kaipanla_dieting where date = '{lastDay}';'''
    result4,_ = dbConnection.Query(sql4)

    zhangTingCount = result2[0][0]
    ZhaBanCount = result3[0][0]
    dieTingCount = result4[0][0]
    if (zhangTingCount+ZhaBanCount) == 0:
        ratio = 0
    else:
        ratio = ZhaBanCount*100.0 / (zhangTingCount+ZhaBanCount)

    sql5 = f'''UPDATE `stock`.`fuPan` SET `两市量` = '{result1[0][0]}', `量比` = '{result1[0][2]}', `增量` = '{result1[0][1]}', `实际涨停` = '{zhangTingCount}', `跌停` = '{dieTingCount}', `炸板` = '{ZhaBanCount}', `炸板率` = '{ratio:.1f}%' WHERE (`日期` = '{lastDay}');'''
    dbConnection.Execute(sql5)



def OneKeyKaiPanLa():
    lastN = 3
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,lastN)

    RequestKaiPanLaVolumnData(dbConnection,tradingDays,lastN)
    RequestKaiPanLaZhangTingData(dbConnection,tradingDays,lastN)
    RequestKaiPanLaZhaBanData(dbConnection,tradingDays,lastN)
    RequestKaiPanLaDieTingData(dbConnection,tradingDays,lastN)
    RequestKaiPanLaZhiRanZhangTingData(dbConnection,tradingDays,lastN)
    MergeDataTo(dbConnection,tradingDays)

if __name__ == "__main__":
    OneKeyKaiPanLa()
