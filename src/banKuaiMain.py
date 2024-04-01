from bankuai.bankuaiMgr import CPercentileBanKuai
from mysql.connect2DB import ConnectToDB
from DBOperating import GetTradingDateLastN




if __name__ == '__main__':
    dbConnection = ConnectToDB()

    p = CPercentileBanKuai(dbConnection)
    tradingDays = GetTradingDateLastN(dbConnection,5)
    p.PercentileBankuai_LastNDays(tradingDays)



