from mysql.connect2DB import ConnectToDB
from Utility.convertDataFrameToJPG import DataFrameToJPG
from DBOperating import GetTradingDateLastN
import pandas as pd
from workspace import workSpaceRoot,GetFuPanRoot,GetStockFolder
import os

def FilterZhangFuMaxZhai(dbConnection,start,end,root = "/tmp/"):
    '''
    选出从 start 到 end 之间 符合可转债筛选条件的 涨幅最大的可转债，按涨幅从大到小排列
    '''
    sql = f'''
    select A.`转债代码`,A.`转债名称`,A.`现价{start}`,B.`现价{end}`,(B.`现价{end}`-A.`现价{start}`) as `delta` from (SELECT `转债代码`,`转债名称`,`现价` as `现价{start}` FROM stock.kezhuanzhai_all where `日期` = "{start}") AS A,(SELECT `转债代码` as `转债代码`,`现价` as `现价{end}` FROM stock.kezhuanzhai_all where `日期` = "{end}") AS B where A.`转债代码` = B.`转债代码` and A.`转债代码` in (SELECT `转债代码` FROM stock.kezhuanzhai where `日期` = "{end}") order by `delta` DESC  
    '''
    print(sql)
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    DataFrameToJPG(df,("转债代码","转债名称"),root,f'''转债区间涨幅排名{end}''')
    df.to_excel(f'''{root}/区间涨幅排名{start} - {end}.xlsx''')

def FilterZhangFuMaxStock(dbConnection,start,end,root = "/tmp/"):
    '''
    选出从 start 到 end 之间 涨幅最大的股票，按涨幅从大到小排列
    '''
    sql = f'''
    select A.`股票代码`,C.`股票简称`,B.`收盘价` AS`收盘价{end}`, A.`最高价` AS `收盘价{start}` from (select * from stock.stockdailyinfo where `日期` = "{start}") AS A,(select * from stock.stockdailyinfo where `日期` = "{end}") AS B,(SELECT * FROM stock.stockbasicinfo) AS C where A.`股票代码` = B.`股票代码` and A.`股票代码` = C.`股票代码`;
    '''
    print(sql)
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    df[f"收盘价{end}"] = df[f"收盘价{end}"].astype(float)
    df[f"收盘价{start}"] = df[f"收盘价{start}"].astype(float)

    df["涨幅"] = (df[f"收盘价{end}"] - df[f"收盘价{start}"]) / df[f"收盘价{start}"] * 100
    df["涨幅"] = df["涨幅"].round(2)
    df = df[df['股票简称'].str.match('[\s\S]*(ST|退)+?[\s\S]*') == False]
    df = df[df['股票代码'].str.match('[\s\S]*(BJ|^688)+?[\s\S]*') == False]

    df.sort_values(f"涨幅",axis=0,ascending=False,inplace=True)
    df.reset_index(inplace=True,drop=True)
    DataFrameToJPG(df,("股票代码","股票简称"),root,f'''股票区间涨幅排名{end}''')
    df.to_excel(f'''{root}/股票区间涨幅排名{start} - {end}.xlsx''')



def FanBaoKeZhuanZai(dbConnection,start,end,root = "/tmp/",name = "可转债反包"):
    '''
    筛选出 end 反包 start 的可转债
    '''
    sql = f'''select A.`转债代码`,A.`转债名称`,B.`现价` AS `现价{end}`, A.`现价` AS `现价{start}`,B.`涨跌幅` AS `涨跌幅{end}`  from (select * from stock.kezhuanzhai_all where `日期` = "{start}") AS A,(select * from stock.kezhuanzhai_all where `日期` = "{end}") AS B where A.`转债代码` = B.`转债代码`;'''
    print(sql)
    if os.path.exists(root) == False:
        os.makedirs(root)
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    df[f"现价{end}"] = df[f"现价{end}"].astype(float)
    df[f"现价{start}"] = df[f"现价{start}"].astype(float)
    df[f"涨跌幅{end}"] = df[f"涨跌幅{end}"].astype(float)
    df = df[df[f"现价{end}"] >= df[f"现价{start}"]]
    df = df[df[f"涨跌幅{end}"] >= 2]
    df.sort_values(f"涨跌幅{end}",axis=0,ascending=False,inplace=True)
    df.reset_index(inplace=True,drop=True)
    DataFrameToJPG(df,("转债代码","转债名称"),root,f'''{name}{start} - {end}''')
    df.to_excel(f'''{root}/可转债反包{start} - {end}.xlsx''')


def _FanBaoKeStock(dbConnection,sql,start,end,root = "/tmp/",name="股票反包"):
    '''
    根据 sql 筛选出 end 反包 start 的股票
    '''
    print(sql)
    if os.path.exists(root) == False:
        os.makedirs(root)
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    df[f"收盘价{end}"] = df[f"收盘价{end}"].astype(float)
    df[f"收盘价{start}"] = df[f"收盘价{start}"].astype(float)
    df[f"涨跌幅{end}"] = df[f"涨跌幅{end}"].astype(float)
    df = df[df[f"收盘价{end}"] >= df[f"收盘价{start}"]]
    df = df[df[f"涨跌幅{end}"] >= 9]
    df = df[df['股票简称'].str.match('[\s\S]*(ST|退)+?[\s\S]*') == False]
    df = df[df['股票代码'].str.match('[\s\S]*(BJ|^688)+?[\s\S]*') == False]
    df.sort_values(f"涨跌幅{end}",axis=0,ascending=False,inplace=True)
    df.reset_index(inplace=True,drop=True)
    DataFrameToJPG(df,("股票代码","股票简称"),root,f'''{name}_{start} - {end}''')
    df.to_excel(f'''{root}/{name}_{start} - {end}.xlsx''')

def FanBaoKeStock_shoupanjia(dbConnection,start,end,root = "/tmp/",name = "股票收盘价反包"):
    '''
    筛选出 end 收盘价反包 start 的股票
    '''
    sql = f'''select A.`股票代码`,C.`股票简称`,B.`收盘价` AS`收盘价{end}`, A.`收盘价` AS `收盘价{start}`,B.`涨跌幅` AS`涨跌幅{end}` from (select * from stock.stockdailyinfo where `日期` = "{start}") AS A,(select * from stock.stockdailyinfo where `日期` = "{end}") AS B,(SELECT * FROM stock.stockbasicinfo) AS C where A.`股票代码` = B.`股票代码` and A.`股票代码` = C.`股票代码`;'''
    
    _FanBaoKeStock(dbConnection,sql,start,end,root,name)

def FanBaoKeStock_zuigaojia(dbConnection,start,end,root = "/tmp/",name = "股票最高价反包"):
    '''
    筛选出 end 最高价反包 start 的股票
    '''
    sql = f'''select A.`股票代码`,C.`股票简称`,B.`收盘价` AS`收盘价{end}`, A.`最高价` AS `收盘价{start}`,B.`涨跌幅` AS`涨跌幅{end}` from (select * from stock.stockdailyinfo where `日期` = "{start}") AS A,(select * from stock.stockdailyinfo where `日期` = "{end}") AS B,(SELECT * FROM stock.stockbasicinfo) AS C where A.`股票代码` = B.`股票代码` and A.`股票代码` = C.`股票代码`;'''
    
    _FanBaoKeStock(dbConnection,sql,start,end,root,name)


def FanBaoNDay(N,root = "/tmp/"):
    '''
    今天反包昨天的，也就是一日反包
    '''
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,5)
    FanBaoKeZhuanZai(dbConnection,tradingDays[-N-1],tradingDays[-1],root,f'''可转债{N}日反包''')
    FanBaoKeStock_zuigaojia(dbConnection,tradingDays[-N-1],tradingDays[-1],root,f'''股票最高价{N}日反包''')
    FanBaoKeStock_shoupanjia(dbConnection,tradingDays[-N-1],tradingDays[-1],root,f'''股票收盘价{N}日反包''')


def ToolMain():
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,3)
    root = GetFuPanRoot(tradingDays[-1])
    root1 = f'''{root}/1日反包/'''
    root2 = f'''{root}/2日反包/'''
    root3 = f'''{root}/3日反包/'''
    FilterZhangFuMaxZhai(dbConnection,"2024-09-20",tradingDays[-1],root)
    FilterZhangFuMaxStock(dbConnection,"2024-10-08",tradingDays[-1],root)
    FanBaoNDay(1,root1)
    FanBaoNDay(2,root2)
    FanBaoNDay(3,root3)


if __name__ == "__main__":
    ToolMain()