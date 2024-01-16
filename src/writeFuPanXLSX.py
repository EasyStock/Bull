import pandas as pd
from writeToExcel.fupanDetail import CFupanDetail
from mysql.connect2DB import ConnectToDB
from fupan.tradingDate import GetTradingDateLastN
from writeToExcel.zhangTingTidui import CWriteZhangTingTiDuiToXLSX
from writeToExcel.zhuanzhai import CZhuanZaiDetail
from writeToExcel.writeZhuanZaiGaiNian import CWriteZhuanZaiGaiNianToXLSX
from writeToExcel.fupanDetailEx import CFupanDetailEx
from writeToExcel.zhangTingJiYing import CWriteZhangTingJiYingToXLSX
from workspace import workSpaceRoot
import os


def WriteFuPanSummaryToXLSX(dbConnection,tradingDays):
    rootFolder = f'''{workSpaceRoot}/复盘/复盘摘要/'''
    if os.path.exists(rootFolder) == False:
        os.makedirs(rootFolder)

    fullPath = os.path.join(rootFolder,f'''复盘摘要{tradingDays[-1]}.xlsx''')
    detail = CFupanDetail(dbConnection,tradingDays)
    zhangTing = CWriteZhangTingTiDuiToXLSX(tradingDays[-1])
    gainian = CWriteZhuanZaiGaiNianToXLSX(dbConnection,tradingDays[-1])
    zhuanzai = CZhuanZaiDetail(dbConnection,tradingDays)
    detailEx = CFupanDetailEx(dbConnection,tradingDays)
    zhangTingJiYing = CWriteZhangTingJiYingToXLSX(dbConnection,tradingDays[-1])
    with pd.ExcelWriter(fullPath,engine='openpyxl',mode='w+') as excelWriter:
        detail.WriteFuPanSummaryToXLSX(excelWriter)
        zhangTing.AnalysisZhangTingReason(dbConnection,excelWriter)
        zhangTing.WriteZhangTingXLSX(dbConnection,excelWriter)
        zhuanzai.WriteZhuanZaiInfoToExcel(excelWriter,2000)
        gainian.WriteZhuanZaiGainToToXLS(excelWriter)
        detailEx.WriteFuPanDetailExToToXLS(excelWriter)
        zhangTingJiYing.WriteZhangTingJiYingToXLS(excelWriter)

def Test1():
    fullPath = "/tmp/aa.xlsx"
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,50)
    detail = CFupanDetail(dbConnection,tradingDays)
    zhangTing = CWriteZhangTingTiDuiToXLSX(tradingDays[-1])
    zhuanzai = CZhuanZaiDetail(dbConnection,tradingDays)
    gainian = CWriteZhuanZaiGaiNianToXLSX(dbConnection,tradingDays[-1])
    detailEx = CFupanDetailEx(dbConnection,tradingDays)
    zhangTingJiYing = CWriteZhangTingJiYingToXLSX(dbConnection,tradingDays[-1])
    with pd.ExcelWriter(fullPath,engine='openpyxl',mode='w+') as excelWriter:
        #detail.WriteFuPanSummaryToXLSX(excelWriter)
        # zhangTing.AnalysisZhangTingReason(dbConnection,excelWriter)
        # zhangTing.WriteZhangTingXLSX(dbConnection,excelWriter)
        # zhuanzai.WriteZhuanZaiInfoToExcel(excelWriter,2000)
        #gainian.WriteZhuanZaiGainToToXLS(excelWriter)
        detailEx.WriteFuPanDetailExToToXLS(excelWriter)
        #zhangTingJiYing.WriteZhangTingJiYingToXLS(excelWriter)


if __name__ == '__main__':
    # dbConnection = ConnectToDB()
    # tradingDays = GetTradingDateLastN(dbConnection,15)
    # WriteFuPanSummaryToXLSX(dbConnection,tradingDays)
    Test1()