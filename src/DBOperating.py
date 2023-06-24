import re
from sqlparse import sql
from mysql.connect2DB import ConnectToDB,ConnectToDB_AliYun,DataFrameToSqls_INSERT_OR_IGNORE
import datetime
import pandas as pd

def GetTradingDateLastN(dbConnection,N):
    today = datetime.date.today()
    end = today.strftime("%Y-%m-%d")
    sql = f"SELECT `日期` FROM stock.treadingDay where `开市` =1 and `交易所`='SSE' and `日期`<='{end}'"
    #print(sql)
    res,_ = dbConnection.Query(sql)
    results = [r[0] for r in res]
    return results[-N:]

def GetZhangTingData(dbConnection,date):
    sql = f"select A.*,B.`所属概念`from `stockZhangting`AS A, `stockBasicInfo` As B where A.`股票代码`= B.`股票代码` and `日期` = '{date}' order by `连续涨停天数` DESC,`首次涨停时间` ASC;"
    #print(sql)
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    #print(df)
    return df

def GetZhangTingDataBy(dbConnection,date,reasons, gaiNians):
    zhangTing = ""
    for index,r, in enumerate(reasons):
        if index == 0:
            zhangTing = zhangTing+ f"A.`涨停原因类别` like '%{r}%' "
        else:
            zhangTing = zhangTing+ " OR "+ f"A.`涨停原因类别` like '%{r}%' "
    
    gai = ""
    for index,r, in enumerate(gaiNians):
        if index == 0:
            gai = gai+ f"B.`所属概念` like '%{r}%' "
        else:
            gai = gai+ " OR "+ f"B.`所属概念` like '%{r}%' "
            
    sql = f"select A.*,B.`所属概念`from `stockZhangting`AS A, `stockBasicInfo` As B where A.`股票代码`= B.`股票代码` and `日期` = '{date}' order by `连续涨停天数` DESC,`首次涨停时间` ASC;"
    if zhangTing != "" and gai != "":
        sql = f"select A.*,B.`所属概念`from `stockZhangting`AS A, `stockBasicInfo` As B where A.`股票代码`= B.`股票代码` and `日期` = '{date}' and ({zhangTing} or {gai}) order by `连续涨停天数` DESC,`首次涨停时间` ASC;"
    else:
        if zhangTing != "":
            sql = f"select A.*,B.`所属概念`from `stockZhangting`AS A, `stockBasicInfo` As B where A.`股票代码`= B.`股票代码` and `日期` = '{date}' and ({zhangTing}) order by `连续涨停天数` DESC,`首次涨停时间` ASC;"
        elif gai != "":
            sql = f"select A.*,B.`所属概念`from `stockZhangting`AS A, `stockBasicInfo` As B where A.`股票代码`= B.`股票代码` and `日期` = '{date}' and ({gai}) order by `连续涨停天数` DESC,`首次涨停时间` ASC;"
            
    #print(sql)
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    return df

def GetRemainZhangTingDataBy(dbConnection,date,reasons, gaiNians):
    zhangTing = ""
    for index,r, in enumerate(reasons):
        if index == 0:
            zhangTing = zhangTing+ f"A.`涨停原因类别` not like '%{r}%' "
        else:
            zhangTing = zhangTing+ " and  "+ f"A.`涨停原因类别` not like '%{r}%' "
    
    gai = ""
    for index,r, in enumerate(gaiNians):
        if index == 0:
            gai = gai+ f"B.`所属概念` not like '%{r}%' "
        else:
            gai = gai+ " and "+ f"B.`所属概念` not like '%{r}%' "
            
    sql = f"select A.*,B.`所属概念`from `stockZhangting`AS A, `stockBasicInfo` As B where A.`股票代码`= B.`股票代码` and `日期` = '{date}' order by `连续涨停天数` DESC,`首次涨停时间` ASC;"
    if zhangTing != "" and gai != "":
        sql = f"select A.*,B.`所属概念`from `stockZhangting`AS A, `stockBasicInfo` As B where A.`股票代码`= B.`股票代码` and `日期` = '{date}' and ({zhangTing}) and ({gai}) order by `连续涨停天数` DESC,`首次涨停时间` ASC;"
    else:
        if zhangTing != "":
            sql = f"select A.*,B.`所属概念`from `stockZhangting`AS A, `stockBasicInfo` As B where A.`股票代码`= B.`股票代码` and `日期` = '{date}' and ({zhangTing}) order by `连续涨停天数` DESC,`首次涨停时间` ASC;"
        elif gai != "":
            sql = f"select A.*,B.`所属概念`from `stockZhangting`AS A, `stockBasicInfo` As B where A.`股票代码`= B.`股票代码` and `日期` = '{date}' and ({gai}) order by `连续涨停天数` DESC,`首次涨停时间` ASC;"
        
    #print(sql)
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    #print(df)
    return df

def Get10CMShouBanZhangTingData(dbConnection,yestoday,today):
    #获取10CM首板涨停
    sql = f"select A.*, B.`股票简称` from `stockDailyInfo` AS A,`stockBasicInfo` As B where A.`股票代码` in (select `股票代码` from `stockZhangting` where `股票代码` REGEXP '^60|^00' and `日期` = '{yestoday}' and `连续涨停天数`=1) and A.`日期` = '{today}' and A.`股票代码` = B.`股票代码` order by A.`涨跌幅` DESC;"
    #print(sql)
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    msg = f"\n==================昨日({yestoday})  10CM首板涨停奖励率========================\n{str(df)}\n {df.shape}"
    return (df,msg)
    
def Get20CMShouBanZhangTingData(dbConnection,yestoday,today):
    #获取20CM首板涨停
    sql = f"select A.*, B.`股票简称` from `stockDailyInfo` AS A,`stockBasicInfo` As B where A.`股票代码` in (select `股票代码` from `stockZhangting` where `股票代码` REGEXP '^30|^68' and `日期` = '{yestoday}' and `连续涨停天数`=1) and A.`日期` = '{today}' and A.`股票代码` = B.`股票代码` order by A.`涨跌幅` DESC;"
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    msg = f"\n==================昨日{yestoday}  20CM首板涨停奖励率========================\n{str(df)}\n {df.shape}"
    return (df,msg)

def Get10CMLianBanZhangTingData(dbConnection,yestoday,today):
    #获取10CM连板涨停
    sql = f"select A.*, B.`股票简称` from `stockDailyInfo` AS A,`stockBasicInfo` As B where A.`股票代码` in (select `股票代码` from `stockZhangting` where `股票代码` REGEXP '^60|^00' and `日期` = '{yestoday}' and `连续涨停天数`>=2) and A.`日期` = '{today}' and A.`股票代码` = B.`股票代码` order by A.`涨跌幅` DESC;"
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    msg = f"\n==================昨日({yestoday})  10CM连板涨停奖励率========================\n {str(df)} \n {df.shape}"
    return (df,msg)

def Get20CMLianBanZhangTingData(dbConnection,yestoday,today):
    #获取20CM连板涨停
    sql = f"select A.*, B.`股票简称` from `stockDailyInfo` AS A,`stockBasicInfo` As B where A.`股票代码` in (select `股票代码` from `stockZhangting` where `股票代码` REGEXP '^30|^68' and `日期` = '{yestoday}' and `连续涨停天数`>=2) and A.`日期` = '{today}' and A.`股票代码` = B.`股票代码` order by A.`涨跌幅` DESC;"
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    msg = f"\n==================昨日({yestoday})  20CM连板涨停奖励率========================\n {str(df)} \n {df.shape}"
    return (df,msg)

def Get1LianBan(dbConnection,today):
    sql = f"select * from `stockZhangting` where `日期` = '{today}' and `连续涨停天数`=1 order by `首次涨停时间`;"
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    msg = f"\n==================今日{today} 首板涨停========================\n {str(df)} \n {df.shape}"
    return (df,msg)

def Get2LianBan(dbConnection,today):
    sql = f"select * from `stockZhangting` where `日期` = '{today}' and `连续涨停天数`=2 order by `首次涨停时间`;"
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    msg = f"\n==================今日{today}  2连板涨停========================\n {str(df)} \n {df.shape}"
    return (df,msg)

def Get3LianBan(dbConnection,today):
    sql = f"select * from `stockZhangting` where `日期` = '{today}' and `连续涨停天数`=3 order by `首次涨停时间`;"
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    msg = f"\n==================今日{today}  3连板涨停========================\n {str(df)} \n {df.shape}"
    return (df,msg)

def Get4AndMoreLianBan(dbConnection,today):
    sql = f"select * from `stockZhangting` where `日期` = '{today}' and `连续涨停天数`>=4 order by `连续涨停天数`DESC,`首次涨停时间` ASC;"
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    msg = f"\n==================今日{today}  4连板及以上涨停========================\n {str(df)} \n {df.shape}"
    return (df,msg)

def GaoWeiFailed(dbConnection,yestoday,today):
    sql = f"select * from `stockZhangting` where `日期` = '{yestoday}' and `连续涨停天数`>=3 and `股票代码` not in (select `股票代码` from `stockZhangting` where `日期` = '{today}' and `连续涨停天数`>=3) order by `连续涨停天数` DESC;"
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    msg = f"\n==================昨日({yestoday})  3板及以上，今日({today})断板========================\n {str(df)} \n {df.shape}"
    return (df,msg)

def ZhangTingFengdan(dbConnection,today):
    sql = f"select * from `stockZhangting` where `日期` = '{today}' order by `连续涨停天数` DESC,`首次涨停时间` ASC;"
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    return df

def DongNeng(dbConnection,yestoday,today):
    sqlY = f"select count(*) from `stockZhangting` where `日期` = '{yestoday}'"
    sqlT = f"select count(*) from `stockZhangting` where `日期` = '{today}' and `连续涨停天数`>=2;"
    resultsY, _ = dbConnection.Query(sqlY)
    resultsT, _ = dbConnection.Query(sqlT)
    #print(resultsY,resultsT)
    if resultsY[0][0] ==0:
        return -1
    else:
        return (resultsT[0][0]/resultsY[0][0]*100)
     

def GetKeZhuanzai(dbConnection,today,gaiNians):
    gai = ""
    for index,r, in enumerate(gaiNians):
        if index == 0:
            gai = gai+ f"B.`所属概念` like '%{r}%' "
        else:
            gai = gai+ " OR "+ f"B.`所属概念` like '%{r}%' "
    
    sql = f"SELECT A.`日期`,A.`转债代码`,A.`转债名称`,A.`现价`,A.`正股名称`,B.`所属概念` FROM `stock`.`kezhuanzhai` as A,`stock`.`stockBasicInfo` AS B where A.`正股名称`=B.`股票简称` and `日期`='{today}' order by `PB` DESC;"
    if gai != "":
        sql = f"SELECT A.`日期`,A.`转债代码`,A.`转债名称`,A.`现价`,A.`正股名称`,B.`所属概念` FROM `stock`.`kezhuanzhai` as A,`stock`.`stockBasicInfo` AS B where A.`正股名称`=B.`股票简称` and `日期`='{today}' and ({gai}) order by `PB` DESC;" 
    
    #print(sql)
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    return df

def GetKeZhuanzai_remain(dbConnection,today,gaiNians):
    gai = ""
    for index,r, in enumerate(gaiNians):
        if index == 0:
            gai = gai+ f"B.`所属概念` not like '%{r}%' "
        else:
            gai = gai+ " and "+ f"B.`所属概念` not like '%{r}%' "
    
    sql = f"SELECTA.`日期`,A.`转债代码`,A.`转债名称`,A.`现价`,A.`正股名称`,B.`所属概念` FROM `stock`.`kezhuanzhai` as A,`stock`.`stockBasicInfo` AS B where A.`正股名称`=B.`股票简称` and `日期`='{today}' order by `PB` DESC;"
    if gai != "":
        sql = f"SELECT A.`日期`,A.`转债代码`,A.`转债名称`,A.`现价`,A.`正股名称`,B.`所属概念` FROM `stock`.`kezhuanzhai` as A,`stock`.`stockBasicInfo` AS B where A.`正股名称`=B.`股票简称` and `日期`='{today}' and ({gai}) order by `PB` DESC;" 
    
    #print(sql)
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    return df

def GetTodayMarketingData(dbConnection,today):
    sql = f"select A.*, B.`股票简称`,B.`上市天数` from `stockDailyInfo` AS A,`stockBasicInfo` As B where A.`股票代码`=B.`股票代码` and `日期`= '{today}' and B.`上市天数`>1;"
    #print(sql)
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    return df

def isTableExist(dbConnection,tableName):
    str.replace
    newName = tableName.replace("`",'')
    names = newName.split('.')
    sql = f''' select * from information_schema.TABLES where TABLE_NAME = "{names[1]}" and TABLE_SCHEMA = "{names[0]}";  '''
    #print(sql)
    results, _ = dbConnection.Query(sql)
    return (len(results) > 0)



def zhuanqianxiaoying_yestoday(dbConnection,yestoday,today):
    # 昨日的赚钱效应， 即昨日涨停股票今天的表现
    sql = f'''SELECT A.*,B.`股票简称`, B.`连续涨停天数` As `昨日连续涨停天数`,B.`涨停原因类别` As `昨日涨停原因类别` FROM stock.stockdailyinfo As A, (SELECT `股票代码`,`股票简称`,`连续涨停天数`,`涨停原因类别` FROM stock.stockzhangting where `日期` = "{yestoday}") As B where A.`日期` = "{today}" and A.`股票代码` = B.`股票代码` order by A.`股票代码`DESC;'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    return df




def migrateDataFrom(srcConnection,destConnection,tableName,drop = False):
    isDestTableExist = isTableExist(destConnection,tableName)
    if isDestTableExist:
        if drop:
            truncateSql = f''' TRUNCATE {tableName}; '''
            #print(truncateSql)
            destConnection.Execute(truncateSql)
            
        querySql = f" select * from {tableName}; "
        results1, columns1 = srcConnection.Query(querySql)
        df = pd.DataFrame(results1,columns=columns1)
        sqls = DataFrameToSqls_INSERT_OR_IGNORE(df,tableName)
        for sql in sqls:
            #print(sql)
            destConnection.Execute(sql)
    else:
        createSql = f'''show create table {tableName};'''
        querySql = f" select * from {tableName}; "
        #print(createSql)
        results,_ = srcConnection.Query(createSql)
        results1, columns1 = srcConnection.Query(querySql)
        df = pd.DataFrame(results1,columns=columns1)
        sqls = DataFrameToSqls_INSERT_OR_IGNORE(df,tableName)
        destConnection.Execute(results[0][1])
        for sql in sqls:
            #print(sql)
            destConnection.Execute(sql)
        

def OneKeyMigrateData(dbConnection,destConnection):
    #migrateDataFrom(dbConnection, destConnection, "`stock`.`treadingday`",True)
    #migrateDataFrom(dbConnection, destConnection, "`stock`.`stockZhangting`",True)
    #migrateDataFrom(dbConnection, destConnection, "`stock`.`stockDailyInfo`",True) # 数据量大
    #migrateDataFrom(dbConnection, destConnection, "`stock`.`stockBasicInfo`",True)
    #migrateDataFrom(dbConnection, destConnection, "`stock`.`nameMapping`",True)
    #migrateDataFrom(dbConnection, destConnection, "`stock`.`kezhuanzhai`",True)
    migrateDataFrom(dbConnection, destConnection, "`stock`.`fuPan`",True)
    
    
    #migrateDataFrom(dbConnection, destConnection, "`stock`.`stockDailyInfo_Tushare`",True)

if __name__ == "__main__":
    dbConnection = ConnectToDB()
    destConnection = ConnectToDB_AliYun()
    # result = GetTradingDateLastN(dbConnection,15)
    # print(result)
    #GetZhangTingDataBy(dbConnection,result[-1],["中药","电子纸"],[])
    #GetZhangTingDataBy(dbConnection,result[-1],[],["中医药","电子纸"])
    
    #GetZhangTingDataBy(dbConnection,result[-1],["中药","电子纸"],["中医药","电子纸"])
    #GetZhangTingDataBy(dbConnection,result[-1],[],[])
    # Get10CMShouBanZhangTingData(dbConnection,result[-2],result[-1])
    # Get20CMShouBanZhangTingData(dbConnection,result[-2],result[-1])
    # Get10CMLianBanZhangTingData(dbConnection,result[-2],result[-1])
    # Get20CMLianBanZhangTingData(dbConnection,result[-2],result[-1])
    # Get2LianBan(dbConnection,result[-1])
    # Get3LianBan(dbConnection,result[-1])
    # Get4AndMoreLianBan(dbConnection,result[-1])
    # GaoWeiFailed(dbConnection,result[-2],result[-1])
    # print(DongNeng(dbConnection,result[-2],result[-1]))
    # print(isTableExist(dbConnection,"stock.treadingDay"))
    OneKeyMigrateData(dbConnection,destConnection)

    