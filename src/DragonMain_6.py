from eastmoney.dragon import CDragonFetcher
from mysql.connect2DB import ConnectToDB
from fupan.tradingDate import GetTradingDateLastN
import time


def FetchDragonDataByDate(date,dbConnection):
    tableName = "`stock`.`dragon`"
    f = CDragonFetcher()
    f.FetchDailyDataAndToDB(date,tableName,dbConnection)



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