import pandas as pd
from FuPanXLSX.fupanDetail import CFupanDetail
from mysql.connect2DB import ConnectToDB
from fupan.tradingDate import GetTradingDateLastN
from FuPanXLSX.zhangTingTidui import CWriteZhangTingTiDuiToXLSX
from workspace import workSpaceRoot
import os


def WriteFuPanSummaryToXLSX(dbConnection,tradingDays):
    rootFolder = f'''{workSpaceRoot}/复盘/复盘摘要/'''
    if os.path.exists(rootFolder) == False:
        os.makedirs(rootFolder)

    fullPath = os.path.join(rootFolder,f'''复盘摘要{tradingDays[-1]}.xlsx''')
    detail = CFupanDetail(dbConnection,tradingDays)
    zhangTing = CWriteZhangTingTiDuiToXLSX(tradingDays[-1])
    with pd.ExcelWriter(fullPath,engine='openpyxl',mode='w+') as excelWriter:
        detail.WriteFuPanSummaryToXLSX(excelWriter)
        zhangTing.AnalysisZhangTingReason(dbConnection,excelWriter)
        zhangTing.WriteZhangTingXLSX(dbConnection,excelWriter)



def Test1():
    fullPath = "/tmp/aa.xlsx"
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,15)
    detail = CFupanDetail(dbConnection,tradingDays)
    zhangTing = CWriteZhangTingTiDuiToXLSX(tradingDays[-1])
    with pd.ExcelWriter(fullPath,engine='openpyxl',mode='w+') as excelWriter:
        detail.WriteFuPanSummaryToXLSX(excelWriter)
        zhangTing.AnalysisZhangTingReason(dbConnection,excelWriter)
        zhangTing.WriteZhangTingXLSX(dbConnection,excelWriter)



if __name__ == '__main__':
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,15)
    WriteFuPanSummaryToXLSX(dbConnection,tradingDays)