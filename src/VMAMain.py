from VMA.VMAMgr import ResetVMAData,UpdateVMAData,TrainAllData,VAMSelector,UpdateVMAData_Process
from mysql.connect2DB import ConnectToDB
from ColoredLog import StartToInitLogger
from DBOperating import GetTradingDateLastN



if __name__ == '__main__':
    dbConnection = ConnectToDB()
    logger = StartToInitLogger("VMA")
    tradingDays = GetTradingDateLastN(dbConnection,5)
    #ResetVMAData(dbConnection) #重置训练数据集表
    #UpdateVMAData(dbConnection,3,True) #更新每日数据表

    UpdateVMAData_Process(dbConnection,-1,False) #更新更新训练集表
    #TrainAllData(dbConnection,VMAs = (60,),gailvThreshold = 80)
    #VAMSelector(dbConnection,tradingDays)