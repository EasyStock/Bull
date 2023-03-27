from mysql.connect2DB import ConnectToDB
from fupan.tradingDate import GetTradingDateLastN
from fupan.jiTianjiBan import GetAllJiTianJiBan,GetJiTianJiBan


def WriteJiTianJiBan():
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,70)
    GetAllJiTianJiBan(dbConnection,tradingDays)
    #GetJiTianJiBan(dbConnection,"603578.SH",tradingDays)

if __name__ == "__main__":
    WriteJiTianJiBan()


