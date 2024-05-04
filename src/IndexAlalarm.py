from mysql.connect2DB import ConnectToDB
import pandas as pd
from MA.MAMgr import CMAMgr
from MA.MACross import CMACross
import numpy as np

def CalcChuangYeBanMACross():
    dbConnection = ConnectToDB()
    sql = f'''SELECT * FROM stock.kaipanla_index where `StockID` = "SZ399006";'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    df.set_index("date",drop=True,inplace=True)
    MAs = [5,10,20,30,60,120,200]
    size = len(MAs)
    data = np.zeros((size, size))
    for i in range(size):
        for j in range(i+1,size):
            cross = CMACross(df,"last_px",MAs[i],MAs[j])
            p = cross.predict()
            print(f'''MA{MAs[i]}-MA{MAs[j]}, {p[0]:.2f},{p[1]:.2f}%''')
            data[i,j] = f'''{p[0]:.2f}'''
            data[j,i] = f'''{p[1]:.2f}'''

    columns = [f'''MA{i}''' for i in MAs]
    df = pd.DataFrame(data,columns= columns,index=columns)
    print(df)

def ChuangYeBanAlarm():
    dbConnection = ConnectToDB()
    sql = f'''SELECT * FROM stock.kaipanla_index where `StockID` = "SZ399006";'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    df.set_index("date",drop=True,inplace=True)
    # mgr = CMAMgr()
    # messages = []
    # res1 = mgr.predict(df,"last_px",(5,10,20,30,60))
    # res2 = mgr.Cross(df,"last_px",5,10)
    # messages.extend(res1)
    # messages.extend(res2)
    #print(messages)
    cross = CMACross(df,"last_px",10,120)
    p = cross.predict()
    print(p)
    cross.Verify(p[1])
    # df["MA5"] = df["last_px"].rolling(window=5).mean()
    # df["MA10"] = df["last_px"].rolling(window=10).mean()
    # df["MA20"] = df["last_px"].rolling(window=20).mean()
    # df["MA30"] = df["last_px"].rolling(window=30).mean()
    # df["MA60"] = df["last_px"].rolling(window=60).mean()
    # df["MA120"] = df["last_px"].rolling(window=120).mean()
    # df["MA200"] = df["last_px"].rolling(window=200).mean()
    df.to_excel("/tmp/创业板.xlsx",index=True)
    # res = cross.FindAllCrossUp()


if __name__ == "__main__":
    #ChuangYeBanAlarm()
    CalcChuangYeBanMACross()