from zhuanzaiMain_1 import GetFromJisiluAndWriteToDB
from thsDataMain_2 import GetTHSData
from fupanMain_3 import FuPanFun
from statistics_4 import Statics
from DragonMain_5 import DragonDaily
from getRecording_6 import GetRecording
from fupanMain_7 import FupanDaily
from GetDuanBanData_B import GetDuanBanData
from thsDataMain2_2 import oneKeyDailyData




if __name__ == "__main__":
    GetFromJisiluAndWriteToDB() #获取每日可转债数据
    #oneKeyDailyData()
    GetTHSData()          # 获取每日股票数据
    FuPanFun() 
    Statics()
    
    GetDuanBanData()
    #GetRecording()
    FupanDaily()
    DragonDaily()
    

    
    
    