from mysql.connect2DB import ConnectToDB
from fupan.tradingDate import GetTradingDateLastN
from fupan.fupanSummary import Summary



def WriteSummary():
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,70)
    Summary(dbConnection,tradingDays)

if __name__ == "__main__":
    WriteSummary()