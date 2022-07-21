from eastmoney.dragon import CDragonFetcher
from mysql.connect2DB import ConnectToDB
from fupan.tradingDate import GetTradingDateLastN
import time
from eastmoney.dragonMonitor import CDragonMonitor
from eastmoney.dragonDupliacte import CDragonDuplicate


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


if __name__ == "__main__":
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

    