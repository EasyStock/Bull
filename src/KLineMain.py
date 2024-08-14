from Stock.KLineMgr import CKLineMgr
from mysql.connect2DB import ConnectToDB


def TestIndex():
    dbConnection = ConnectToDB()
    sql = 'SELECT A.* , B.`股票简称` FROM stock.stockdailyinfo As A, stock.stockbasicinfo AS B where A.`日期` = "2024-08-06" and A.`股票代码` = B.`股票代码`;'
    sql = f'''SELECT * FROM stock.index_dailyinfo where `指数代码` = "000001.SH" and `日期` >= "2022-01-10" ;'''
    lines = CKLineMgr.ReadFromDB(dbConnection, sql)
    for line in lines:
        #print(line)
        if line.isBigRedLine(5) or line.isPinBar():
            print(line)


def TestStockInfo():
    dbConnection = ConnectToDB()
    sql = 'SELECT A.* , B.`股票简称` FROM stock.stockdailyinfo As A, stock.stockbasicinfo AS B where A.`日期` = "2024-08-06" and A.`股票代码` = B.`股票代码`;'
    sql = f'''SELECT * FROM stock.index_dailyinfo where `指数代码` = "000001.SH" and `日期` >= "2022-01-10" ;'''
    lines = CKLineMgr.ReadFromDB(dbConnection, sql)
    for line in lines:
        #print(line)
        if line.isBigRedLine(5) or line.isPinBar():
            print(line)

if __name__ == "__main__":
    pass

    

