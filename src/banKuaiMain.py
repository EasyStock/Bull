from bankuai.bankuaiMgr import CPercentileBanKuai
from mysql.connect2DB import ConnectToDB
from DBOperating import GetTradingDateLastN


if __name__ == '__main__':
    dbConnection = ConnectToDB()

    #AnalysisBanKuai(dbConnection)
    p = CPercentileBanKuai(dbConnection)
    tradingDays = GetTradingDateLastN(dbConnection,3)
    p.PercentileBankuai_LastNDays(tradingDays)


