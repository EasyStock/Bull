from mysql.connect2DB import ConnectToDB
from fupan.tradingDate import GetTradingDateLastN
import pandas as pd
import os


def ConvertDataFrameToJPG(df,fullPath):
    from pandas.plotting import table
    import matplotlib.pyplot as plt
    plt.rcParams["font.sans-serif"] = ["SimHei"]#显示中文字体
    high = int(0.174 * df.shape[0]+0.5)+1
    fig = plt.figure(figsize=(3, high), dpi=200)#dpi表示清晰度
    ax = fig.add_subplot(111, frame_on=False) 
    ax.xaxis.set_visible(False)  # hide the x axis
    ax.yaxis.set_visible(False)  # hide the y axis
    table(ax, df, loc='center')  # 将df换成需要保存的dataframe即可
    plt.savefig(fullPath)


def _getDataBySql(dbConnection,sql):
    data, columns = dbConnection.Query(sql)
    df = pd.DataFrame(data,columns=columns)
    return df


def GetData_duanban(dbConnection,yesteday,today):
    sql = f'''select * from stock.stockzhangting where `日期` = "{yesteday}"  and `股票代码` not in (SELECT `股票代码` FROM stock.stockzhangting where `日期` = "{today}") order by `连续涨停天数` DESC, `首次涨停时间` ASC,`最终涨停时间` ASC;'''

    fodler = f'/home/jenkins/复盘/股票/{today}/'
    if os.path.exists(fodler) == False:
        os.makedirs(fodler)
    
    fileName1 = f'''断板_{today}'''

    df = _getDataBySql(dbConnection,sql)
    print(df)
    DataFrameToJPG(df,["股票代码","股票简称"],fodler,fileName1)



def DataFrameToJPG(df,columns,rootPath, fileName):
    size = df.shape[0]
    step = 80
    if size > step:
        for index in range(0,size,step):
            tmp = df.iloc[index:,]
            if index + step <= size:
                tmp = df.iloc[index:index+step,]
            fullPath = f"{rootPath}{fileName}_{int(index/step+1)}.jpg"
            print(fullPath)
            jpgDataFrame = pd.DataFrame(tmp,columns=columns)
            ConvertDataFrameToJPG(jpgDataFrame,fullPath)
    else:
        fullPath = f"{rootPath}{fileName}.jpg"
        print(fullPath)
        jpgDataFrame = pd.DataFrame(df,columns=columns)
        ConvertDataFrameToJPG(jpgDataFrame,fullPath)



def GetDuanBanData():
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,70)
    GetData_duanban(dbConnection,tradingDays[-2],tradingDays[-1])

if __name__ == "__main__":
    GetDuanBanData()


