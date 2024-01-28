from VMA.VMAMgr import ResetVMAData,UpdateVMAData,TrainAllData
from mysql.connect2DB import ConnectToDB
from ColoredLog import StartToInitLogger

if __name__ == '__main__':
    dbConnection = ConnectToDB()
    logger = StartToInitLogger("VMA")
    #ResetVMAData(dbConnection) #重置训练数据集表
    #UpdateVMAData(dbConnection,3,True) #更新每日数据表
    #UpdateVMAData(dbConnection,200,False) #更新更新训练集表
    TrainAllData(dbConnection,VMAs = (60,),gailvThreshold = 80)