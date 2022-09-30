from mysql.connect2DB import ConnectToDB
from fupan.tradingDate import GetTradingDateLastN
import pandas as pd
import os


def ConvertDataFrameToJPG(df,fullPath):
    from pandas.plotting import table
    import matplotlib.pyplot as plt
    plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']#显示中文字体
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


def GetDataByGaiNian(dbConnection,gainian,date):
    sql1 = f'''SELECT A.*,B.`连续涨停天数` FROM stock.stockbasicinfo As A, stockzhangting As B where A.`所属概念` like "%{gainian}%" and A.`股票代码` = B.`股票代码` and A.`更新日期` = B.`日期` order by B.`连续涨停天数` DESC,B.`首次涨停时间` ASC,B.`最终涨停时间` ASC;'''
    sql2 = f'''SELECT A.`日期`,A.`转债代码`,A.`转债名称`,A.`现价`,A.`正股名称`,B.`所属概念` FROM `stock`.`kezhuanzhai` as A,`stock`.`stockBasicInfo` AS B where A.`正股名称`=B.`股票简称` and `日期`='{date}' and (B.`所属概念` like '%{gainian}%'  OR B.`所属概念` like '%{gainian}%' ) order by `PB` DESC;'''
    sql3 = f'''select * from stock.stockbasicinfo where `所属概念` like "%{gainian}%" ;'''

    fodler = f'/Volumes/Data/复盘/股票/{date}/'
    if os.path.exists(fodler) == False:
        os.makedirs(fodler)
    
    fileName1 = f'''{gainian}_股票_涨停_{date}'''
    fileName2 = f'''{gainian}_可转债_{date}'''
    fileName3 = f'''{gainian}_股票_All_{date}'''

    df = _getDataBySql(dbConnection,sql1)
    print(df)
    DataFrameToJPG(df,["股票代码","股票简称"],fodler,fileName1)


    df = _getDataBySql(dbConnection,sql2)
    print(df)
    if not df.empty:
        DataFrameToJPG(df,["转债代码","转债名称"],fodler,fileName2)

    df = _getDataBySql(dbConnection,sql3)
    print(df)
    DataFrameToJPG(df,["股票代码","股票简称"],fodler,fileName3)
    


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



if __name__ == "__main__":
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,70)
    GetDataByGaiNian(dbConnection,"医疗器械",tradingDays[-1])


