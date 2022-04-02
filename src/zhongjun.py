from ColoredLog import StartToInitLogger
from mysql.connect2DB import ConnectToDB
from DBOperating import GetTradingDateLastN
import pandas as pd


def NewHigh(data,N=100):
    if data.shape[0]<N:
        return False
    
    t= data["收盘价"].astype(float)
    max = t.max()
    last1 = t.iloc[-1]
    if(last1 >= max):
        return True
    else:
        return False


def FetchData(dbConnection,lastN):
    lastNDates = GetTradingDateLastN(dbConnection,lastN)
    print(lastNDates)
    sql = f''' SELECT * FROM stock.stockdailyinfo where `日期` >= "{lastNDates[0]}";'''
    print(sql)
    datas,columns = dbConnection.Query(sql)
    df = pd.DataFrame(datas,columns=columns)
    print(df)
    t = df.groupby(["股票代码"])
    for k in t:
        stockID = k[0]
        data = k[1]

        if NewHigh(data,lastN):
            print(stockID)
        #input()
    
    
def Test():
    dbConnection = ConnectToDB()
    FetchData(dbConnection,36)




if __name__ == "__main__":
    Test()