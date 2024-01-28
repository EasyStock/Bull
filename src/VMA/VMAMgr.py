from VMA.VMAData import CResetVMAData,CUpdateVMAData
from VMA.VMADataTraining import CVMADataTraining
import logging

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
        input()