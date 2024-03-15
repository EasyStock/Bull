from mysql.connect2DB import ConnectToDB
import pandas as pd
from Utility.convertDataFrameToJPG import DataFrameToJPG
import numpy as np


def CalcPercentail():
    pd.set_option('display.unicode.ambiguous_as_wide',True)
    pd.set_option('display.unicode.east_asian_width',True)
    pd.set_option('display.width',360)
    dbConnection = ConnectToDB()
    sql = '''SELECT * FROM stock.fuPan;'''
    data, columns = dbConnection.Query(sql)
    df = pd.DataFrame(data=data,columns=columns)
    df.dropna(inplace= True)
    newColunms = ["红盘","两市量","实际涨停","跌停","炸板","炸板率","连板","10CM首板奖励率","10CM连板奖励率","首板个数","2连板个数","3连板个数","4连板及以上个数","高度板","动能","势能"]
    newDF = pd.DataFrame(df,columns=newColunms)
    newDF['炸板率'] = newDF['炸板率'].apply(lambda x:x[:-1]).astype(float)
    newDF['两市量'] = newDF['两市量'].apply(lambda x:x[:-1]).astype(float)

    for c in newColunms:
        newDF[c] = newDF[c].astype(float)

    t = newDF.quantile([0.02, 0.05, 0.1, 0.5,0.9, 0.95, 0.98])
    
    print(t)
    t.to_excel('/tmp/Percentail1.xlsx')

    newColunms1 = ["首板率","连板率","昨日首板溢价率","昨日首板晋级率","昨日2板溢价率","昨日2板晋级率","涨停数量","连板数量","收-5数量","大盘红盘比","亏钱效应","首板红盘比","首板大面比","连板股的红盘比","连板比例","连板大面比","昨日连板未涨停数的绿盘比","势能EX","动能EX"]
    newDF2 = pd.DataFrame(df,columns=newColunms1)
    s = newDF2.quantile([0.02, 0.05, 0.1, 0.5,0.9, 0.95, 0.98])
    print(s)
    s.to_excel('/tmp/Percentail2.xlsx')

def CalcPercentail1():
    pd.set_option('display.unicode.ambiguous_as_wide',True)
    pd.set_option('display.unicode.east_asian_width',True)
    pd.set_option('display.width',360)
    dbConnection = ConnectToDB()
    sql = '''SELECT * FROM stock.kezhuanzhai_all where `日期` = "2023-12-26"'''
    data, columns = dbConnection.Query(sql)
    df = pd.DataFrame(data=data,columns=columns)
    df.dropna(inplace= True)
    newColunms = ["日期","转债代码","转债名称","现价","成交额(万元)","PB","剩余规模"]
    newDF = pd.DataFrame(df,columns=newColunms)
    newDF.set_index(["日期","转债代码","转债名称"],drop=True,inplace=True)
    newDF['剩余规模'] = newDF['剩余规模'].apply(lambda x:x[:-1]).astype(float)
    newDF['剩余规模'] = newDF['剩余规模']*10000
    newDF["成交额/剩余规模"] = newDF["成交额(万元)"]/newDF["剩余规模"]
    newDF.dropna()
    # newDF['炸板率'] = newDF['炸板率'].apply(lambda x:x[:-1]).astype(float)
    # newDF['两市量'] = newDF['两市量'].apply(lambda x:x[:-1]).astype(float)

    # for c in newColunms:
    #     newDF[c] = newDF[c].astype(float)

    t = newDF.quantile([0.02, 0.05, 0.1, 0.5,0.9, 0.95, 0.98])
    
    print(t)
    print(newDF)
    newDF.to_excel('/tmp/Percentail2.xlsx')

def _score(volumn,percentail,reversed = False):
    result = 1
    for index, value in percentail.items():
        if float(volumn) <= float(value):
            result = float(index)
            break
    
    if reversed:
        result = 1.0 - float(result)
    
    result = int(result * 100)
    return result
    
def CalcPercentail3():
    pd.set_option('display.unicode.ambiguous_as_wide',True)
    pd.set_option('display.unicode.east_asian_width',True)
    pd.set_option('display.width',360)
    dbConnection = ConnectToDB()
    startDay = "2024-02-05"
    endDay = "2024-03-12"
    key1 = f'''收盘价{startDay}'''
    key2 = f'''收盘价{endDay}'''
    #sql = f'''select  A.`股票代码`, C.`股票简称` ,A.`收盘价`, B.`收盘价` as `收盘价2` from stock.stockdailyinfo As A, (SELECT * FROM stock.stockdailyinfo where `日期` = "{startDay}") As B ,(SELECT * FROM stock.stockbasicinfo) As C where A.`股票代码` = B.`股票代码` and A.`股票代码` = C.`股票代码` and A.`日期` = "{endDay}";'''
    sql = f'''SELECT A.`转债代码` As `股票代码`,A.`转债名称`  As `股票简称` , A.`现价` As `{key2}`,B.`现价` As `{key1}` FROM stock.kezhuanzhai_all As A,(SELECT `转债代码`, `现价` FROM stock.kezhuanzhai_all where `日期` = "{startDay}") As B where A.`日期` = "{endDay}" and A.`转债代码` = B.`转债代码`;'''
    
    data, columns = dbConnection.Query(sql)
    df = pd.DataFrame(data=data,columns=columns)
    df.dropna()
    df[key1] =  df[key1].astype(float)
    df[key2] =  df[key2].astype(float)
    #df['涨跌价格'] = (df[key2] - df[key1])
    df['涨跌幅'] = (df[key2] - df[key1])/df[key1]*100
    # df = df[df['股票简称'].str.match('[\s\S]*(ST)+?[\s\S]*') == False]
    # df = df[df['股票代码'].str.match('[\s\S]*(BJ|^688)+?[\s\S]*') == False]
    df.sort_values('涨跌幅',axis=0,ascending=False,inplace=True)
    
    DataFrameToJPG(df,("股票代码","股票简称"),"/tmp/","Percentail3")
    df.set_index(["股票代码","股票简称"],drop=True,inplace=True)
    step = list(np.linspace(0, 1, 101))
    t = df.quantile(step)
    #df["涨跌价格分数"] = df.apply(lambda row: _score(row["涨跌价格"],t["涨跌价格"],False), axis=1)
    df["涨跌幅分数"] = df.apply(lambda row: _score(row["涨跌幅"],t["涨跌幅"],False), axis=1)
    df.reset_index(inplace=True)
    #df.reset_index(drop=True,inplace=True)
    df.to_excel('/tmp/Percentail3.xlsx')
    
    print(t)

if __name__ == "__main__":
    CalcPercentail3()
