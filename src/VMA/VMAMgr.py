from VMA.VMAData import CResetVMAData,CUpdateVMAData
from VMA.VMADataTraining import CVMADataTraining
from VMA.VMASelecter import CVMASelecter
import logging
from mysql.connect2DB import ConnectToDB
import multiprocessing

def MultiThreadUpdateVDAData(index,stockID,lastN = -1,isDailyData =False):
    dbConnection = ConnectToDB()
    update = CUpdateVMAData(dbConnection,stockID)
    if isDailyData == True:
        update.UpdateDailyDataVMA(lastN)
    else:
        update.UpdateTrainingDataVMA(lastN)
    
    msg = f"========={index}===================={stockID} Finished================================"
    print(msg)
    return msg


def UpdateVMAData_Process(dbConnection,lastN = -1,isDailyData = False):
    sql = "SELECT `股票代码` FROM stock.stockbasicinfo ;"
    results, _ = dbConnection.Query(sql)

    ret = []
    pool = multiprocessing.Pool(64)
    size = len(results)
    for index,res in enumerate(results):
        stockID = res[0]
        msg = f'''index: {index+1} / {size}'''
        ret.append(pool.apply_async(MultiThreadUpdateVDAData, (msg,stockID, lastN,isDailyData)))

    pool.close()
    pool.join()
    for r in ret:
        logging.warning(r._value)


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
    for date in tradingDays:
        selector = CVMASelecter(dbConnection)
        selector.Select(date)