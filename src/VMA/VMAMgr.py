#!/usr/bin/env python3
from VMA.VMAData import CResetVMAData,CUpdateVMAData
from VMA.VMADataTraining import CVMADataTraining
from VMA.VMASelecter import CVMASelecter
import logging
from mysql.connect2DB import ConnectToDB
import multiprocessing
import datetime

def MultiThreadUpdateVDAData_Proc(index,stockID,startTime,lastN = -1,isDailyData =False):
    dbConnection = ConnectToDB()
    update = CUpdateVMAData(dbConnection,stockID)
    if isDailyData == True:
        update.UpdateDailyDataVMA(lastN)
    else:
        update.UpdateTrainingDataVMA(lastN)
    
    endTime = datetime.datetime.now()
    msg = f"======={index}==更新数据:{stockID} Finished======从开始到现在花费时间:{str(endTime - startTime)}==========="
    print(msg)
    return msg


def MultiThreadTrainingVDAData_Proc(index,stockID,startTime,VMAs,gailvThreshold):
    dbConnection = ConnectToDB()
    for vma in VMAs:
        train = CVMADataTraining(dbConnection,stockID)
        train.Training(vma,gailvThreshold)

    endTime = datetime.datetime.now()
    msg = f"===={index}===训练:{stockID} Finished======从开始到现在花费时间:{str(endTime - startTime)}============="
    print(msg)
    return msg

############################################################################################
def UpdateVMAData_Process(dbConnection,lastN = -1,isDailyData = False):
    sql = "SELECT `股票代码` FROM stock.stockbasicinfo ;"
    results, _ = dbConnection.Query(sql)

    startTime = datetime.datetime.now()
    ret = []
    pool = multiprocessing.Pool(64)
    size = len(results)
    for index,res in enumerate(results):
        stockID = res[0]
        msg = f'''index: {index+1} / {size} = {100.0*(index+1)/size:.2f}% '''
        ret.append(pool.apply_async(MultiThreadUpdateVDAData_Proc, (msg,stockID, startTime,lastN,isDailyData)))

    pool.close()
    pool.join()
    for r in ret:
        logging.warning(r._value)

def TrainAllData_MultiProcess(dbConnection,VMAs = (30,60,),gailvThreshold = 70):
    sql = "SELECT `股票代码` FROM stock.stockbasicinfo;"
    results, _ = dbConnection.Query(sql)

    startTime = datetime.datetime.now()
    ret = []
    pool = multiprocessing.Pool(64)
    size = len(results)
    for index,res in enumerate(results):
        stockID = res[0]
        msg = f'''index: {index+1} / {size} = {100.0*(index+1)/size:.2f}% '''
        ret.append(pool.apply_async(MultiThreadTrainingVDAData_Proc, (msg,stockID, startTime,VMAs,gailvThreshold)))

    pool.close()
    pool.join()
    for r in ret:
        logging.warning(r._value)


################################################################################################################################
        
def ResetVMAData(dbConnection):
    reset = CResetVMAData(dbConnection)
    reset.Reset()


def UpdateVMAData(dbConnection,lastN = -1,isDailyData = False):
    sql = "SELECT `股票代码` FROM stock.stockbasicinfo;"
    results, _ = dbConnection.Query(sql)
    size = len(results)
    for index,re in enumerate(results):
        stockID = re[0]
        logging.warning(f"==========================={index+1}/{size}====={stockID}================================")
        update = CUpdateVMAData(dbConnection,stockID)
        if isDailyData == True:
            update.UpdateDailyDataVMA(lastN)
        else:
            update.UpdateTrainingDataVMA(lastN)

def TrainAllData(dbConnection,VMAs = (30,60,),gailvThreshold = 70):
    sql = "SELECT `股票代码` FROM stock.stockbasicinfo;"
    results, _ = dbConnection.Query(sql)
    size = len(results)
    for index,re in enumerate(results):
        stockID = re[0]
        logging.warning(f"==========================={index+1}/{size}====={stockID}================================")
        for vma in VMAs:
            train = CVMADataTraining(dbConnection,stockID)
            train.Training(vma,gailvThreshold)
        #input()
            

def VAMSelector(dbConnection,tradingDays):
    VMAs = {
        "60":2,
        "90":2,
    }
    for date in tradingDays:
        for vma in VMAs:
            selector = CVMASelecter(dbConnection)
            selector.Select(date,vma,VMAs[vma])


def updateChangWei(dbConnection):
    train = CVMADataTraining(dbConnection,None)
    train.UpdateCangwei()