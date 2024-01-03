from kaipanla.kaipanlaDataMgr import RequestVolumnDataByDates,RequestZhangTingDataByDates,RequestZhaBanDataByDates,RequestDieTingDataByDates,RequestZhiRanZhangTingDataByDates,RequestIndexData,RequestZhangDieTingJiashu
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
# def RequestKaiPanLaZhangTingData(dbConnection,tradingDays,lastN = 3):
#     RequestZhangTingDataByDates(tradingDays,dbConnection)

def RequestKaiPanLaZhaBanData(dbConnection,tradingDays,lastN = 3):
    res = RequestZhaBanDataByDates(tradingDays,dbConnection)
    for date in res:
        ZhaBanCount = res[date]
        sql5 = f'''UPDATE `stock`.`fuPan` SET `炸板` = '{ZhaBanCount}' WHERE (`日期` = '{date}');'''
        print(sql5)
        dbConnection.Execute(sql5)
    print("\n")
# def RequestKaiPanLaDieTingData(dbConnection,tradingDays,lastN = 3):
#     RequestDieTingDataByDates(tradingDays,dbConnection)


# def RequestKaiPanLaZhiRanZhangTingData(dbConnection,tradingDays,lastN = 3):
#     RequestZhiRanZhangTingDataByDates(tradingDays,dbConnection)


# def RequestKaiPanLaIndexData(dbConnection,tradingDays,lastN = 3):
#     RequestIndexData(tradingDays,dbConnection)

def RequestKaiPanLaZhangDieTingJiashu(dbConnection,tradingDays,lastN = 3):
    res = RequestZhangDieTingJiashu(tradingDays,dbConnection)
    for date in res:
        zhangTingCount, dieTingCount = res[date]
        sql5 = f'''UPDATE `stock`.`fuPan` SET  `实际涨停` = '{int(zhangTingCount)}', `跌停` = '{int(dieTingCount)}'  WHERE (`日期` = '{date}');'''
        print(sql5)
        dbConnection.Execute(sql5)
    print("\n")

# def MergeDataTo(dbConnection,tradingDays):
#     lastDay = tradingDays[-1]
#     sql1  = f'''select volumn,delta,ratio from stock.kaipanla_volumn where date = "{lastDay}";'''
#     result1,_ = dbConnection.Query(sql1)
#     if len(result1) == 0:
#         return

#     sql2 = f'''SELECT count(*) FROM stock.kaipanla_zhangting where date = '{lastDay}';'''
#     result2,_ = dbConnection.Query(sql2)

#     sql3 = f'''SELECT count(*) FROM stock.kaipanla_zhaban where date = '{lastDay}';'''
#     result3,_ = dbConnection.Query(sql3)
    
#     sql4 = f'''SELECT count(*) FROM stock.kaipanla_dieting where date = '{lastDay}';'''
#     result4,_ = dbConnection.Query(sql4)

#     zhangTingCount = result2[0][0]
#     ZhaBanCount = result3[0][0]
#     dieTingCount = result4[0][0]
#     if (zhangTingCount+ZhaBanCount) == 0:
#         ratio = 0
#     else:
#         ratio = ZhaBanCount*100.0 / (zhangTingCount+ZhaBanCount)

#     sql5 = f'''UPDATE `stock`.`fuPan` SET `两市量` = '{result1[0][0]}', `量比` = '{result1[0][2]}', `增量` = '{result1[0][1]}', `实际涨停` = '{zhangTingCount}', `跌停` = '{dieTingCount}', `炸板` = '{ZhaBanCount}', `炸板率` = '{ratio:.1f}%' WHERE (`日期` = '{lastDay}');'''
#     print(sql5)
#     dbConnection.Execute(sql5)

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


    #RequestKaiPanLaZhangTingData(dbConnection,tradingDays,lastN)
    #RequestKaiPanLaDieTingData(dbConnection,tradingDays,lastN)
    #RequestKaiPanLaZhiRanZhangTingData(dbConnection,tradingDays,lastN)
    #RequestKaiPanLaIndexData(dbConnection,tradingDays,lastN)
    #MergeDataTo(dbConnection,tradingDays)


def Test():
    lastN = 1000
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,lastN)
    RequestIndexData(tradingDays,dbConnection)

if __name__ == "__main__":
    OneKeyKaiPanLa()
