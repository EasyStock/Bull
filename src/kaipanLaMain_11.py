from kaipanla.kaipanlaDataMgr import RequestVolumnDataByDates,RequestZhaBanDataByDates,RequestIndexData,RequestZhangDieTingJiashu
from mysql.connect2DB import ConnectToDB
from fupan.tradingDate import GetTradingDateLastN
import urllib3
import datetime
import pytz

urllib3.disable_warnings()

def RequestKaiPanLaVolumnData(dbConnection,tradingDays,lastN = 3):
    res = RequestVolumnDataByDates(tradingDays,dbConnection)
    for date in res:
        liangshiLiang, zhengliang, ratio = res[date]
        sql5 = f'''UPDATE `stock`.`fuPan` SET `两市量` = '{liangshiLiang}', `量比` = '{ratio}', `增量` = '{zhengliang}' WHERE (`日期` = '{date}');'''
        print(sql5)
        dbConnection.Execute(sql5)
    print("\n")

def RequestKaiPanLaZhaBanData(dbConnection,tradingDays,lastN = 3):
    for tradingDay in tradingDays:
        sql = f'''UPDATE `stock`.`fuPan` SET `炸板` = (SELECT count(*) FROM stock.stockdaily_zhaban where `日期` = "{tradingDay}") WHERE (`日期` = "{tradingDay}");'''
        dbConnection.Execute(sql)
    # res = RequestZhaBanDataByDates(tradingDays,dbConnection)
    # for date in res:
    #     ZhaBanCount = res[date]
    #     sql5 = f'''UPDATE `stock`.`fuPan` SET `炸板` = '{ZhaBanCount}' WHERE (`日期` = '{date}');'''
    #     print(sql5)
    #     dbConnection.Execute(sql5)
    print("\n")

def RequestKaiPanLaZhangDieTingJiashu(dbConnection,tradingDays,lastN = 3):
    res = RequestZhangDieTingJiashu(tradingDays,dbConnection)
    for date in res:
        zhangTingCount, dieTingCount = res[date]
        sql5 = f'''UPDATE `stock`.`fuPan` SET  `实际涨停` = '{int(zhangTingCount)}', `跌停` = '{int(dieTingCount)}'  WHERE (`日期` = '{date}');'''
        print(sql5)
        dbConnection.Execute(sql5)
    print("\n")

def CalcZhaBanRatio(dbConnection,tradingDays):
    #计算炸板率
    for treadingDay in tradingDays:
        sql = f'''SELECT `实际涨停`,`炸板` FROM stock.fupan WHERE (`日期` = '{treadingDay}');'''
        result2,_ = dbConnection.Query(sql)
        if len(result2) == 0:
            continue
        zhangTingCount = int(result2[0][0])
        zhaBanCount = int(result2[0][1])
        ratio = 0
        if (zhangTingCount+zhaBanCount) != 0:
            ratio = zhaBanCount*100.0 / (zhangTingCount+zhaBanCount)
        sql5 = f'''UPDATE `stock`.`fuPan` SET `炸板率` = '{ratio:.1f}%' WHERE (`日期` = '{treadingDay}');'''
        print(sql5)
        dbConnection.Execute(sql5)
    print("\n")

def OneKeyKaiPanLa():
    lastN = 3
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,lastN)
    today = datetime.datetime.now(pytz.timezone('Asia/Shanghai')).date()
    print("现在UTC时间是:",datetime.datetime.utcnow())
    if tradingDays[-1] == str(today) and datetime.datetime.utcnow().time() < datetime.time(8, 30):
        tradingDays = tradingDays[:-1]

    RequestKaiPanLaVolumnData(dbConnection,tradingDays,lastN)
    RequestKaiPanLaZhangDieTingJiashu(dbConnection,tradingDays,lastN)
    RequestKaiPanLaZhaBanData(dbConnection,tradingDays,lastN)
    CalcZhaBanRatio(dbConnection,tradingDays)
    RequestIndexData(tradingDays,dbConnection)

def Test():
    lastN = 1000
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,lastN)
    RequestIndexData(tradingDays,dbConnection)

if __name__ == "__main__":
    OneKeyKaiPanLa()
