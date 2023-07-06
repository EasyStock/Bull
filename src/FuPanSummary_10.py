from mysql.connect2DB import ConnectToDB
from fupan.tradingDate import GetTradingDateLastN
from fupan.fupanSummary import Summary
from fupan.fupanSummary2 import CFupanSummary



def WriteSummary():
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,70)
    Summary(dbConnection,tradingDays)


def Test():
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,70)
    fupanSummary = CFupanSummary(dbConnection,tradingDays[-1])
    fupanSummary.WirteFupanSummary()

if __name__ == "__main__":
    Test()