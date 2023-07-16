from zhuanzaiMain_1 import GetFromJisiluAndWriteToDB
from thsDataMain_2 import GetTHSData
from fupanMain_3 import FuPanFun
from statistics_4 import Statics
from DragonMain_5 import DragonDaily
from getRecording_6 import GetRecording
from fupanMain_7 import FupanDaily
from GetDuanBanData_B import GetDuanBanData
from thsDataMain2_2 import oneKeyDailyData
from ColoredLog import StartToInitLogger

from GetJiTianJiBan_9 import WriteJiTianJiBan
from FuPanSummary_10 import WriteSummary
from kaipanLaMain_11 import OneKeyKaiPanLa
from ifindMain import refreshAccessToken,FengDan


def AllInOne():
    logger = StartToInitLogger("AllInOne")
    #GetFromJisiluAndWriteToDB(logger) #获取每日可转债数据

    oneKeyDailyData(logger)  
    #GetTHSData()          # 获取每日股票数据     
    FuPanFun(logger) 
    Statics()
    
    GetDuanBanData()
    #GetRecording()
    FupanDaily()
    DragonDaily()
    WriteJiTianJiBan()
    OneKeyKaiPanLa()
    refreshAccessToken()
    FengDan()
    WriteSummary()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    helpStr = f'''
    0 : AllInOne
    1 : 可转债
    2 : 同花顺每日数据
    3 : 每日复盘1
    4 : 统计
    5 : 断板数据
    6 : 每日复盘2
    7 : 每日龙虎榜
    8 : 计算几天几板
    9 : 开盘啦数据
    10 : 写复盘总结
    11 : 刷新Token
    12 : 写一字板封单量
    '''
    logger = StartToInitLogger("AllInOne")
    parser.add_argument('-i','--index',default=0,choices=[0,1,2,3,4,5,6,7,8,9,10,11,12],type=int, help=helpStr)
    args = parser.parse_args()

    if args.index == 0:
        AllInOne()
    elif args.index == 1:
        GetFromJisiluAndWriteToDB(logger)
    elif args.index == 2:
        oneKeyDailyData(logger)
    elif args.index == 3:
        FuPanFun(logger)
    elif args.index == 4:
        Statics()
    elif args.index == 5:
        GetDuanBanData()
    elif args.index == 6:
        FupanDaily()
    elif args.index == 7:
        DragonDaily()
    elif args.index == 8:
        WriteJiTianJiBan()
    elif args.index == 9:
        OneKeyKaiPanLa()
    elif args.index == 10:
        WriteSummary()
    elif args.index == 11:
        refreshAccessToken()
    elif args.index == 12:
        FengDan()
    else:
        pass
    
    
