import pandas as pd
from FuPanXLSX.fupanDetail import CFupanDetail
from mysql.connect2DB import ConnectToDB
from fupan.tradingDate import GetTradingDateLastN

def Test1():
    fullPath = "/tmp/aa.xlsx"
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,15)
    detail = CFupanDetail(dbConnection,tradingDays)
    with pd.ExcelWriter(fullPath,engine='openpyxl',mode='w+') as excelWriter:
        detail.WriteFuPanSummaryToXLSX(excelWriter)



if __name__ == '__main__':
    Test1()