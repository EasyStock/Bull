from bankuai.bankuaiMgr import CPercentileBanKuai
from mysql.connect2DB import ConnectToDB


if __name__ == '__main__':
    dbConnection = ConnectToDB()
    #GetAllValueableBanKuaiData(dbConnection)
    #AnalysisBanKuai(dbConnection)
    p = CPercentileBanKuai(dbConnection)
    p.PercentileBanKuai()


