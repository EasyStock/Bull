from eastmoney.dragon import CDragonFetcher
from mysql.connect2DB import ConnectToDB
from fupan.tradingDate import GetTradingDateLastN
import time
from eastmoney.dragonMonitor import CDragonMonitor
from eastmoney.dragonDupliacte import CDragonDuplicate
from eastmoney.beijiaosuo import CBeiJiaoSuoDataFetcher


def FetchDragonDataByDate(date,dbConnection):
    tableName = "`stock`.`dragon`"
    f = CDragonFetcher()
    f.FetchDailyDataAndToDB(date,tableName,dbConnection)


def DragonMonitor(date,dbConnection):
    tableName = "`stock`.`dragon`"
    monitor = CDragonMonitor(dbConnection,tableName,date)
    monitor.DoMonitor()

def DragonMonitor_ALL(tradingDays,dbConnection):
    index = 0
    t = reversed(tradingDays)
    for date in t:
        DragonMonitor(date,dbConnection)
        index = index + 1


def DragonDuplicate(date,dbConnection):
    tableName = "`stock`.`dragon`"
    tableName_guanlian = "`stock`.`dragon_guanlian`"
    duplicate = CDragonDuplicate(dbConnection,tableName,tableName_guanlian,date)
    duplicate.GetData()


def DragonDuplicate_ALL(tradingDays,dbConnection):
    index = 0
    t = reversed(tradingDays)
    for date in t:
        DragonDuplicate(date,dbConnection)
        index = index + 1



def DragonDaily():
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,599)
    FetchDragonDataByDate(tradingDays[-1],dbConnection)
    DragonMonitor(tradingDays[-1],dbConnection)
    DragonDuplicate(tradingDays[-1],dbConnection)


def Test():
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,599)
    #print(tradingDays)
    # index = 0
    # t = reversed(tradingDays[:-520])
    # for date in t:
    #     print(f"==========={index}:{date}=============\n")
    #     index = index + 1
    #     FetchDragonDataByDate(date,dbConnection)
    #     time.sleep(10)

    FetchDragonDataByDate(tradingDays[-1],dbConnection)
    DragonMonitor(tradingDays[-1],dbConnection)
    DragonDuplicate(tradingDays[-1],dbConnection)

    #DragonMonitor_ALL(tradingDays,dbConnection)
    #DragonDuplicate_ALL(tradingDays,dbConnection)
    #   
if __name__ == "__main__":
    #DragonDaily()
    fetcher = CBeiJiaoSuoDataFetcher()
    dbConnection = ConnectToDB()
    #fetcher.FetcheData(dbConnection)
    for momey in range(1000000,10000000,100000):
        year = 2021
        shouyi1, shouyi2, shouyi3 = fetcher.shouYi(dbConnection,year,momey)
        print(f"{year} 年: 投资金额: {momey/10000:.1f} 万元, 平均价收益: {shouyi1:.2f},平均价收益率{shouyi1/momey*100:.2f}%       开盘价收益: {shouyi2:.2f}, 开盘价收益率{shouyi2/momey*100:.2f}% ,         收盘价收益: {shouyi3:.2f} 收盘价收益率{shouyi3/momey*100:.2f}%")


    