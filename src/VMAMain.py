#!/usr/bin/env python3
from VMA.VMAMgr import ResetVMAData,UpdateVMAData,TrainAllData,TrainAllData_MultiProcess,VAMSelector,UpdateVMAData_Process
from mysql.connect2DB import ConnectToDB
from ColoredLog import StartToInitLogger
from DBOperating import GetTradingDateLastN
import datetime
import pytz


def Main():
    logger = StartToInitLogger("VMA")
    dbConnection = ConnectToDB()
    now = datetime.datetime.now(pytz.timezone('Asia/Shanghai'))
    logger.error("现在是北京时间是:"+str(now))

    import argparse
    parser = argparse.ArgumentParser()
    helpStr = f'''
    -o : options
    '''
    parser.add_argument('-o','--options', action="store",default=True,nargs="+",type=int, help="")
    args = parser.parse_args()
    #args.options = [5,]
    
    if 0 in args.options:
        ResetVMAData(dbConnection) #重置训练数据集表
    
    if 1 in args.options:
        UpdateVMAData_Process(dbConnection,-1,False) #更新更新训练集表

    if 2 in args.options:
        TrainAllData_MultiProcess(dbConnection,VMAs = (60,),gailvThreshold = 80)

    if 3 in args.options:
        UpdateVMAData_Process(dbConnection,-1,True) #更新每日数据表

    if 4 in args.options:
        UpdateVMAData_Process(dbConnection,3,True) #更新每日数据表

    if 5 in args.options:
        tradingDays = GetTradingDateLastN(dbConnection,1)
        VAMSelector(dbConnection,tradingDays)

if __name__ == '__main__':
    dbConnection = ConnectToDB()
    logger = StartToInitLogger("VMA")
    # tradingDays = GetTradingDateLastN(dbConnection,5)
    # #
    #UpdateVMAData(dbConnection,3,True) #更新每日数据表

    # #UpdateVMAData_Process(dbConnection,-1,False) #更新更新训练集表
    # UpdateVMAData_Process(dbConnection,3,True) #更新每日数据表
    #TrainAllData_MultiProcess(dbConnection,VMAs = (60,),gailvThreshold = 80)
    # #
    Main()