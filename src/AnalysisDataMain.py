from ColoredLog import StartToInitLogger
from mysql.connect2DB import ConnectToDB
from fupan.tradingDate import GetTradingDateLastN

from zhuanzai.jisilu import CJiSiLu
from categrate import CATEGRAGTE_KE_ZHUAN_ZAI
from zhuanzai.compareIndex import CCompareWithIndex
from fupan.yiziban import CYizhiban
from fupan.zhuanQianXiaoying import CZhuanQianXiaoXing
from fupan.fupanSummary2 import CFupanSummary

from fupanMain_3 import CFuPan,NewGaiNian
from statistics_4 import categrateZhangTing,AnalysisZhangTingReason
from fupanMain_7 import PrintSQLs,GetFuPanList,GetZhouqiGaoBiao
from fupan.jiTianjiBan import GetAllJiTianJiBan
from kaipanLaMain_11 import RequestKaiPanLaVolumnData,RequestKaiPanLaZhangDieTingJiashu,RequestKaiPanLaZhaBanData,CalcZhaBanRatio
from message.feishu.webhook_zhuanzai import SendKeZhuanZaiYuJing,SendKeZhuanZaiNewGaiNian,Send5DaysKeZhuanZaiNewGaiNian,SendNDaysKeZhuanZaiQiangShu,SendNewStocks,SendReDianOfToday
from message.feishu.webhook_stock import SendNewGaiNianOfStock,SendMeiRiFuPan_Stock
from writeFuPanXLSX import WriteFuPanSummaryToXLSX

import pytz
import datetime
import time

def _updateKaiPanLaData(dbConnection,tradingDays,lastN = 3):
    #获取开盘啦的两市成交量数据、涨跌停家数、炸板数、咋板率数据
    if len(tradingDays) > lastN:
        tradingDays = tradingDays[-lastN:]

    RequestKaiPanLaVolumnData(dbConnection,tradingDays,lastN)
    RequestKaiPanLaZhangDieTingJiashu(dbConnection,tradingDays,lastN)
    RequestKaiPanLaZhaBanData(dbConnection,tradingDays,lastN)
    CalcZhaBanRatio(dbConnection,tradingDays)

#####################################以下是入口函数######################################################
def AnalysisDataOfKeZhuanZhai(dbConnection,tradingDays,logger):
    logger.info(f'==============开始分析可转债:{datetime.datetime.now(pytz.timezone("Asia/Shanghai"))}==============================')
    #可转债各个概念归类
    jisiLu = CJiSiLu(logger,dbConnection)
    jisiLu.Categrate(CATEGRAGTE_KE_ZHUAN_ZAI)

    #和指数相比，计算强弱
    comparer = CCompareWithIndex(dbConnection,logger)
    if len(tradingDays) > 5:
        tradingDays = tradingDays[-5:]
    comparer.CompareWithIndex_ALL(tradingDays)
    logger.info(f'==============结束分析可转债:{datetime.datetime.now(pytz.timezone("Asia/Shanghai"))}==============================\n')

def AnalysisDataOfStock(dbConnection,tradingDays,logger):
    logger.info(f'==============开始分析股票:{datetime.datetime.now(pytz.timezone("Asia/Shanghai"))}==============================')
    #分享各个层级将利率，红盘率，计算动能势能
    FuPan = CFuPan(logger,dbConnection,-1)
    FuPan.MarketingData()
    FuPan.FuPan()
    FuPan.FormatFuPanSqlAndToDB()
    logger.info(str(FuPan))

    #分析新增概念
    NewGaiNian(dbConnection)

    #分析涨停原因
    AnalysisZhangTingReason(dbConnection)
    #涨停原因归类
    categrateZhangTing(dbConnection)

    #一字板
    yiziban = CYizhiban(dbConnection,tradingDays[-1])
    yiziban.YiZhiBan()

    #赚钱效应
    zhuanq = CZhuanQianXiaoXing(dbConnection,tradingDays[-2],tradingDays[-1])
    zhuanq.ZhuanQianXiaoYing()

    #打印对应的复盘SQL
    PrintSQLs(tradingDays)
    
    #获取首板，2板，3板，4板及4板以上个股
    GetFuPanList(dbConnection,tradingDays[-1])

    #获取周期高标
    GetZhouqiGaoBiao(dbConnection,tradingDays[-45],tradingDays[-1])

    #写上几天几板数据
    GetAllJiTianJiBan(dbConnection,tradingDays)
    
    _updateKaiPanLaData(dbConnection,tradingDays,3)
    
    #写复盘总结
    fupanSummary = CFupanSummary(dbConnection,tradingDays[-1])
    fupanSummary.WirteFupanSummary()

    #写复盘摘要到Excel
    WriteFuPanSummaryToXLSX(dbConnection,tradingDays)
    logger.info(f'==============结束分析股票:{datetime.datetime.now(pytz.timezone("Asia/Shanghai"))}==============================\n')

def SendReportOfKeZhuanZai(dbConnection,tradingDays,logger):
    logger.info(f'==============开始发送可转债分析结果:{datetime.datetime.now(pytz.timezone("Asia/Shanghai"))}==============================')
    groups = [
        ("https://open.feishu.cn/open-apis/bot/v2/hook/4901573e-b858-434a-a787-5faa28982b1a","brYyzPbSks4OKnMgdwKvIh"), #测试API
        #("https://open.feishu.cn/open-apis/bot/v2/hook/cddb5b9f-d4e5-48b2-862a-37f77c44a0a5","sUNRDcO4erOybcDNaAD8Hb"), #可转债免费推送群
        #("https://open.feishu.cn/open-apis/bot/v2/hook/c0a8b97e-6817-49df-a653-2116e4e30fdd","QOWgXDc90zhxN7fVuULL9f"), #可转债付费推送群
        
    ]
    for group in groups:
        webhook = group[0]
        secret = group[1]
        #可转债预警
        SendKeZhuanZaiYuJing(dbConnection,tradingDays,webhook,secret)
        #可转债新概念
        SendKeZhuanZaiNewGaiNian(dbConnection,tradingDays,webhook,secret)
        #可转债新概念5日总结
        Send5DaysKeZhuanZaiNewGaiNian(dbConnection,tradingDays,webhook,secret)
        #可转债强赎
        SendNDaysKeZhuanZaiQiangShu(dbConnection,tradingDays,webhook,secret,-5)
        #北交所打新
        SendNewStocks(dbConnection,tradingDays,webhook,secret)
        #今日热点
        SendReDianOfToday(dbConnection,tradingDays,webhook,secret)
    logger.info(f'==============结束发送可转债分析结果:{datetime.datetime.now(pytz.timezone("Asia/Shanghai"))}==============================\n')


def SendReportOfStock(dbConnection,tradingDays,logger):
    logger.info(f'==============开始发送股票分析结果:{datetime.datetime.now(pytz.timezone("Asia/Shanghai"))}==============================')
    groups = [
        ("https://open.feishu.cn/open-apis/bot/v2/hook/4901573e-b858-434a-a787-5faa28982b1a","brYyzPbSks4OKnMgdwKvIh"), #测试API
        #("https://open.feishu.cn/open-apis/bot/v2/hook/9c3b588e-a528-4c92-a9dd-a44e29abf2fb","VaT4TcvmILA0lYAv9cjbcc"), #每日复盘群
    ]
    for group in groups:
        webhook = group[0]
        secret = group[1]
        SendNewGaiNianOfStock(dbConnection,tradingDays,webhook,secret)
        time.sleep(3)
        SendMeiRiFuPan_Stock(dbConnection,tradingDays,webhook,secret)
    logger.info(f'==============结束发送股票分析结果:{datetime.datetime.now(pytz.timezone("Asia/Shanghai"))}==============================\n')


#####################################################################################################
if __name__ == "__main__":
    logger = StartToInitLogger("分析数据入口")
    dbConnection = ConnectToDB()
    lastN = 50
    tradingDays = GetTradingDateLastN(dbConnection,lastN)
    now = datetime.datetime.now(pytz.timezone('Asia/Shanghai'))
    print("现在是北京时间是:",now)

    import argparse
    parser = argparse.ArgumentParser()
    helpStr = f'''
    -o : options
    '''
    parser.add_argument('-o','--options', action="store",default=True,nargs="+",type=int, help="0.分析并发布分析结果 1. 分析可转债数据, 2. 分析股票数据 3. 发送可转债分析结果, 发送股票分析结果")
    args = parser.parse_args()
    #args.options = [0,1,2,3,4]

    if 0 in args.options:
        ######################
        #分析可转债数据
        AnalysisDataOfKeZhuanZhai(dbConnection,tradingDays,logger)

        #分析股票数据
        AnalysisDataOfStock(dbConnection,tradingDays,logger)

        #发送可转债分析结果
        SendReportOfKeZhuanZai(dbConnection,tradingDays,logger)

        #发送股票分析结果
        SendReportOfStock(dbConnection,tradingDays,logger)
        ######################
    else:
        if 1 in args.options:
            #分析可转债数据
            AnalysisDataOfKeZhuanZhai(dbConnection,tradingDays,logger)

        if 2 in args.options:
            #分析股票数据
            AnalysisDataOfStock(dbConnection,tradingDays,logger)

        if 3 in args.options:
            #发送可转债分析结果
            SendReportOfKeZhuanZai(dbConnection,tradingDays,logger)
            
        if 4 in args.options:
            #发送股票分析结果
            SendReportOfStock(dbConnection,tradingDays,logger)
