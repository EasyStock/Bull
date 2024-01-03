from ColoredLog import StartToInitLogger
from mysql.connect2DB import ConnectToDB
from DBOperating import GetTradingDateLastN
import pandas as pd

from workspace import WorkSpaceFont

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


def moshi1(data):
    listDate = data["上市天数"].astype(int)
    listDate1 =  listDate.iloc[-1]
    if listDate1 < 150:
        return False

    try:
        # close = data["收盘价"].astype(float)
        # open = data["开盘价"].astype(float)
        v =  data["成交量"].astype(float)
        v1 = v.iloc[-1]
        v2 = v.iloc[-2]
        v3 = v.iloc[-3]
        v4 = v.iloc[-4]
        
        if (v2 > 2*v1) and (v3 > 2*v1) and (v2 > 2*v4) and (v3 > 2*v4):
            return True
    except:
        return False
    
    return False
    

def ConvertDataFrameToJPG(df,fullPath):
    from pandas.plotting import table
    import matplotlib.pyplot as plt
    plt.rcParams["font.sans-serif"] = [WorkSpaceFont]#显示中文字体
    high = int(0.174 * df.shape[0]+0.5) +1
    fig = plt.figure(figsize=(3, high), dpi=400)#dpi表示清晰度
    ax = fig.add_subplot(111, frame_on=False) 
    ax.xaxis.set_visible(False)  # hide the x axis
    ax.yaxis.set_visible(False)  # hide the y axis
    table(ax, df, loc='center')  # 将df换成需要保存的dataframe即可
    plt.savefig(fullPath)
    plt.close()

def FetchData(dbConnection,lastN):
    lastNDates = GetTradingDateLastN(dbConnection,lastN)
    print(lastNDates)
    #sql = f''' SELECT A.*,B.`股票简称`,B.`上市天数` FROM stock.stockdailyinfo As A, `stock`.`stockBasicInfo` AS B where A.`股票代码` = B.`股票代码` and A.`股票代码` NOT REGEXP '^8.*' and `日期` >= "{lastNDates[0]}";'''
    sql = f'''SELECT `转债代码`,`转债名称` FROM stock.kezhuanzhai where `日期`='2022-04-07' order by `PB` DESC;'''
    print(sql)
    datas,columns = dbConnection.Query(sql)
    df = pd.DataFrame(datas,columns=columns)
    size = df.shape[0]
    step = 40
    if size > step:
        for index in range(0,size,step):
            tmp = df.iloc[index:,]
            if index + step <= size:
                tmp = df.iloc[index:index+step,]
            fullPath = f"/tmp/bb_{int(index/step+1)}.jpg"
            print(fullPath)
            ConvertDataFrameToJPG(tmp,fullPath)

    else:
        ConvertDataFrameToJPG(df,"/tmp/aa.jpg")
    print(df)
    
    # t = df.groupby(["股票代码"])
    # for k in t:
    #     stockID = k[0]
    #     data = k[1]
    #     stockName = data["股票简称"].iloc[-1]
    #     #print(data)
    #     if moshi1(data):
    #         #print("====AAAAA====")
    #         print(stockID,stockName)
            #input()
        # if NewHigh(data,lastN):
        #     print(stockID)
        # input()
    
    
def Test():
    dbConnection = ConnectToDB()
    FetchData(dbConnection,36)




if __name__ == "__main__":
    Test()
