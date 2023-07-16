from mysql.connect2DB import ConnectToDB
from ifind.token import CIFindToken
from ifind.fengdan import CFengdan
from DBOperating import GetTradingDateLastN

def refreshAccessToken():
    dbConnection = ConnectToDB()
    finder = CIFindToken(dbConnection)
    finder.refreshAccessToken()

def FengDan():
    dbConnection = ConnectToDB()
    finder = CIFindToken(dbConnection)
    finder.GetTokenInfoFromDB()
    token = finder.access_token

    fengdan = CFengdan(dbConnection,token)
    #fengdan.Query(["000151.SZ"],"2023-06-16")
    tradingDays = GetTradingDateLastN(dbConnection,30)
    fengdan.ProcessFengDanLastN(tradingDays)

if __name__ == "__main__":
    #refreshAccessToken()
    FengDan()
