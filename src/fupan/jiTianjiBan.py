import pandas as pd
import os



def GetJiTianJiBan(dbConnection,stockID,tradingDays):
    if len(tradingDays) < 30:
        raise "所给的时间期限不足30天"
    sqlDays = '''","'''.join(tradingDays[-13:])
    #print(sqlDays)
    sql = f'''select `日期` from  stock.stockzhangting where `日期`  in ("{sqlDays}") and `股票代码` = "{stockID}"  order by `日期` DESC;'''
    #print(sql)
    res,_ = dbConnection.Query(sql)
    results = [r[0] for r in res]

    sql2 = f'''SELECT `日期` FROM stock.stockdailyinfo where `股票代码` = "{stockID}" order by `日期` DESC limit 30;'''
    res2,_ = dbConnection.Query(sql2)
    actuallyTradingDays = [r[0] for r in res2]
    startIndex = actuallyTradingDays.index(results[0])
    endIndex = actuallyTradingDays.index(results[-1])
    tmp = actuallyTradingDays[startIndex:endIndex+1]
    #print(tmp)
    msg = f'''{len(tmp)}天{len(results)}板'''
    return msg



def GetAllJiTianJiBan(dbConnection,tradingDays):
    sql = f'''select `股票代码` from  stock.stockzhangting where `日期` = "{tradingDays[-1]}" order by `连续涨停天数` DESC,`最终涨停时间` ASC;'''
    res,_ = dbConnection.Query(sql)
    stockIDs = [r[0] for r in res]
    for stockID in stockIDs:
        msg = GetJiTianJiBan(dbConnection,stockID,tradingDays)
        newSql = f'''UPDATE `stock`.`stockzhangting` SET `涨停关键词` = '{msg}' WHERE (`日期` = '{tradingDays[-1]}') and (`股票代码` = '{stockID}');'''
        print(newSql)
        dbConnection.Execute(newSql)


