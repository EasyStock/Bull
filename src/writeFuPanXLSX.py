import pandas as pd
from writeToExcel.fupanDetail import CFupanDetail
from mysql.connect2DB import ConnectToDB
from fupan.tradingDate import GetTradingDateLastN
from writeToExcel.zhangTingTidui import CWriteZhangTingTiDuiToXLSX
from writeToExcel.zhuanzhai import CZhuanZaiDetail
from writeToExcel.writeZhuanZaiGaiNian import CWriteZhuanZaiGaiNianToXLSX
from writeToExcel.fupanDetailEx import CFupanDetailEx
from writeToExcel.zhangTingJiYing import CWriteZhangTingJiYingToXLSX
from workspace import workSpaceRoot,GetFuPanRoot
from bankuai.bankuaiMgr import CBanKuaiSelect
import os
from writeToExcel.chuangYeBanMA import CChuangYeBanMACross
from writeToExcel.yeWu import CWriteYeWuToXLSX


def WriteFuPanSummaryToXLSX(dbConnection,tradingDays):
    rootFolder =os.path.join(GetFuPanRoot(tradingDays[-1]),"XLSX")
    if os.path.exists(rootFolder) == False:
        os.makedirs(rootFolder)

    fullPath = os.path.join(rootFolder,f'''复盘摘要{tradingDays[-1]}.xlsx''')
    detail = CFupanDetail(dbConnection,tradingDays)
    zhangTing = CWriteZhangTingTiDuiToXLSX(tradingDays[-1])
    gainian = CWriteZhuanZaiGaiNianToXLSX(dbConnection,tradingDays[-1])
    zhuanzai = CZhuanZaiDetail(dbConnection,tradingDays)
    detailEx = CFupanDetailEx(dbConnection,tradingDays)
    zhangTingJiYing = CWriteZhangTingJiYingToXLSX(dbConnection,tradingDays[-1])
    banKuaiSelect = CBanKuaiSelect(dbConnection)
    chuangyeban = CChuangYeBanMACross(dbConnection,tradingDays)
    with pd.ExcelWriter(fullPath,engine='openpyxl',mode='w+') as excelWriter:
        detail.WriteFuPanSummaryToXLSX(excelWriter)
        banKuaiSelect.BanKuaiSelect(excelWriter,tradingDays[-5:],50)
        zhangTing.AnalysisZhangTingReason(dbConnection,excelWriter)
        zhangTing.WriteZhangTingXLSX(dbConnection,excelWriter)
        chuangyeban.WriteChuangYeBanMACrossToToXLS(excelWriter)
        zhuanzai.WriteZhuanZaiInfoToExcel(excelWriter,2000)
        gainian.WriteZhuanZaiGainToToXLS(excelWriter)
        detailEx.WriteFuPanDetailExToToXLS(excelWriter)
        zhangTingJiYing.WriteZhangTingJiYingToXLS(excelWriter)
        

def WriteZhaiYeWuSummaryToXLSX(dbConnection,tradingDays):
    rootFolder =os.path.join(GetFuPanRoot(tradingDays[-1]),"XLSX")
    if os.path.exists(rootFolder) == False:
        os.makedirs(rootFolder)
    fullPath = os.path.join(rootFolder,f'''可转债主营业务与概念{tradingDays[-1]}.xlsx''')
    yewu1 = CWriteYeWuToXLSX(dbConnection,tradingDays[-1])
    yewu1.sheetName = f'''可转债主营业务(全)'''
    yewu1.title = f'''可转债(全)主营业务表 ({tradingDays[-1]})'''

    yewu2 = CWriteYeWuToXLSX(dbConnection,tradingDays[-1])
    yewu2.sheetName = f'''可转债主营业务(符合条件)'''
    yewu2.title = f'''可转债(符合条件)主营业务表 ({tradingDays[-1]})'''
    yewu2.sql = f'''select  A.`转债名称`, B.`股票简称`,B.`行业`, B.`产品`,B.`范围` from (select `转债代码`, `转债名称`, `正股代码`, `正股名称` from kezhuanzhai where `日期` = "{tradingDays[-1]}")  AS A, (SELECT * FROM stock.stockyewu) AS B where A.`正股名称` = B.`股票简称`'''
    
    gainian = CWriteZhuanZaiGaiNianToXLSX(dbConnection,tradingDays[-1])
    with pd.ExcelWriter(fullPath,engine='openpyxl',mode='w+') as excelWriter:
        yewu1.WriteYeWuToToXLS(excelWriter)
        yewu2.WriteYeWuToToXLS(excelWriter)
        gainian.WriteZhuanZaiGainToToXLS(excelWriter)

def WriteStockYeWuSummaryToXLSX(dbConnection,tradingDays):
    rootFolder =os.path.join(GetFuPanRoot(tradingDays[-1]),"XLSX")
    if os.path.exists(rootFolder) == False:
        os.makedirs(rootFolder)

    fullPath = os.path.join(rootFolder,f'''股票主营业务与概念{tradingDays[-1]}.xlsx''')

    yewu1 = CWriteYeWuToXLSX(dbConnection,tradingDays[-1])
    yewu1.sheetName = f'''主板股票主营业务'''
    yewu1.title = f'''主板股票主营业务表 ({tradingDays[-1]})'''
    yewu1.sql = f'''SELECT `股票代码`,`股票简称`,`行业`,`产品`,`范围` FROM stock.stockyewu where `股票代码` REGEXP '^60';'''


    yewu2 = CWriteYeWuToXLSX(dbConnection,tradingDays[-1])
    yewu2.sheetName = f'''中小板股票主营业务'''
    yewu2.title = f'''中小板股票主营业务表 ({tradingDays[-1]})'''
    yewu2.sql = f'''SELECT `股票代码`,`股票简称`,`行业`,`产品`,`范围` FROM stock.stockyewu where `股票代码` REGEXP '^00';'''


    yewu3 = CWriteYeWuToXLSX(dbConnection,tradingDays[-1])
    yewu3.sheetName = f'''创业板股票主营业务'''
    yewu3.title = f'''创业板股票主营业务表 ({tradingDays[-1]})'''
    yewu3.sql = f'''SELECT `股票代码`,`股票简称`,`行业`,`产品`,`范围` FROM stock.stockyewu where `股票代码` REGEXP '^30';'''


    yewu4 = CWriteYeWuToXLSX(dbConnection,tradingDays[-1])
    yewu4.sheetName = f'''科创板股票主营业务'''
    yewu4.title = f'''科创板股票主营业务表 ({tradingDays[-1]})'''
    yewu4.sql = f'''SELECT `股票代码`,`股票简称`,`行业`,`产品`,`范围` FROM stock.stockyewu where `股票代码` REGEXP '^68';'''

    yewu5 = CWriteYeWuToXLSX(dbConnection,tradingDays[-1])
    yewu5.sheetName = f'''北交所股票主营业务'''
    yewu5.title = f'''北交所股票主营业务表 ({tradingDays[-1]})'''
    yewu5.sql = f'''SELECT `股票代码`,`股票简称`,`行业`,`产品`,`范围` FROM stock.stockyewu where `股票代码` NOT REGEXP '^60|^00|^30|^68';'''

    with pd.ExcelWriter(fullPath,engine='openpyxl',mode='w+') as excelWriter:
        yewu1.WriteYeWuToToXLS(excelWriter)
        yewu2.WriteYeWuToToXLS(excelWriter)
        yewu3.WriteYeWuToToXLS(excelWriter)
        yewu4.WriteYeWuToToXLS(excelWriter)
        yewu5.WriteYeWuToToXLS(excelWriter)

def WriteToXLSXMain(dbConnection, tradingDays):
    WriteFuPanSummaryToXLSX(dbConnection, tradingDays)
    WriteZhaiYeWuSummaryToXLSX(dbConnection, tradingDays)
    WriteStockYeWuSummaryToXLSX(dbConnection, tradingDays)


def Test1():
    fullPath = "/tmp/bb.xlsx"
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,50)
    # detail = CFupanDetail(dbConnection,tradingDays)
    # zhangTing = CWriteZhangTingTiDuiToXLSX(tradingDays[-1])
    zhuanzai = CZhuanZaiDetail(dbConnection,tradingDays)
    # gainian = CWriteZhuanZaiGaiNianToXLSX(dbConnection,tradingDays[-1])
    # detailEx = CFupanDetailEx(dbConnection,tradingDays)
    # zhangTingJiYing = CWriteZhangTingJiYingToXLSX(dbConnection,tradingDays[-1])
    # banKuaiSelect = CBanKuaiSelect(dbConnection)
    chuangyeban = CChuangYeBanMACross(dbConnection,tradingDays)
    yewu = CWriteYeWuToXLSX(dbConnection,tradingDays[-1])
    with pd.ExcelWriter(fullPath,engine='openpyxl',mode='w+') as excelWriter:
        #detail.WriteFuPanSummaryToXLSX(excelWriter)
        # zhangTing.AnalysisZhangTingReason(dbConnection,excelWriter)
        # zhangTing.WriteZhangTingXLSX(dbConnection,excelWriter)
        #zhuanzai.WriteZhuanZaiInfoToExcel(excelWriter,2000)
        #gainian.WriteZhuanZaiGainToToXLS(excelWriter)
        #detailEx.WriteFuPanDetailExToToXLS(excelWriter)
        #zhangTingJiYing.WriteZhangTingJiYingToXLS(excelWriter)
        #banKuaiSelect.BanKuaiSelect(excelWriter,tradingDays[-5:],50)
        #chuangyeban.WriteChuangYeBanMACrossToToXLS(excelWriter)
        yewu.WriteYeWuToToXLS(excelWriter)
        


if __name__ == '__main__':
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,15)
    # WriteFuPanSummaryToXLSX(dbConnection,tradingDays)
    #Test1()
    WriteStockYeWuSummaryToXLSX(dbConnection,tradingDays)