from message.feishu.webhook_api import sendMessageByWebhook
from message.feishu.messageformat_feishu import FormatCardOfZhuanZaiYuJing,FormatCardOfNewGaiNian, FormatCardOfNewGaiNian5Days,FormatCardOfQiangShu,FormatCardOfNewStock,FormatCardOfReDianToday,FormatCardOfKeZhuanZaiScore,FormatCardOfKeZhuanZaiPingJiChanged
import pandas as pd
import json
import re

def SendKeZhuanZaiYuJing(dbConnection,tradingDays,webhook,secret):
    # 可转债不符合条件预警
    sql = f'''
    select `转债代码`,`转债名称`,`筛选结果`as `原因`  FROM stock.kezhuanzhai_all where `日期` = "{tradingDays[-1]}" and `转债代码` in
            (select `转债代码` FROM stock.kezhuanzhai where `日期` = "{tradingDays[-2]}" and  `转债代码` not in (select `转债代码` FROM stock.kezhuanzhai where `日期` = "{tradingDays[-1]}") 
            UNION
            select `转债代码` FROM stock.kezhuanzhai where `日期` = "{tradingDays[-2]}" and  `转债代码` in (select `转债代码` FROM stock.kezhuanzhai where `日期` = "{tradingDays[-1]}" and `正股名称`  like "%ST%") # and `正股名称` not like "%ST%"
            )   
         '''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    if df.empty:
        return

    t = FormatCardOfZhuanZaiYuJing(tradingDays[-1],df)
    if t is None:
        return
    content = json.dumps(t,ensure_ascii=False)
    msg_type = "interactive"
    sendMessageByWebhook(webhook,secret,msg_type,content)

def SendKeZhuanZaiNewGaiNian(dbConnection,tradingDays,webhook,secret):
    sql = f'''SELECT * FROM stock.gainian where `概念名称` not in (SELECT `概念名称` FROM stock.gainian where `更新日期`="{tradingDays[-2]}") and `更新日期`="{tradingDays[-1]}";'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    if df.empty:
        return

    for result in results:
        gainian = result[0]
        sql1 = f'''INSERT IGNORE INTO `stock`.`stockgainiannew` (`日期`, `新概念`) VALUES ('{tradingDays[-1]}', '{gainian}');'''
        dbConnection.Execute(sql1)

        sql2 = f'''SELECT A.`转债代码`,A.`转债名称` FROM `stock`.`kezhuanzhai_all` as A,`stock`.`stockBasicInfo` AS B where A.`正股名称`=B.`股票简称` and `日期`='{tradingDays[-1]}' and B.`所属概念` like '%{gainian}%' order by `PB` DESC;'''
        results2, _ = dbConnection.Query(sql2)
        msg = FormatCardOfNewGaiNian(tradingDays[-1],gainian,results2,["**转债代码**","**转债名称**"],showDetails=True)
        content = json.dumps(msg,ensure_ascii=False)
        msg_type = "interactive"
        sendMessageByWebhook(webhook,secret,msg_type,content)

        s = ";".join([f'''{t[0]} {t[1]}''' for t in results2])
        sql3 = f'''UPDATE `stock`.`stockgainiannew` SET `可转债` = '{s}' WHERE (`日期` = '{tradingDays[-1]}') and (`新概念` = '{gainian}');'''
        dbConnection.Execute(sql3)


def Send5DaysKeZhuanZaiNewGaiNian(dbConnection,tradingDays,webhook,secret):
    # 近5日新增加概念和可转债
    last5Days = tradingDays[-5]
    sql = f'''SELECT `日期`,`新概念`  FROM stock.stockgainiannew where `日期` >= "{last5Days}";'''
    results, _ = dbConnection.Query(sql)
    if len(results) == 0:
        return
    
    data = []
    for result in results:
        date = result[0]
        gaiNian = result[1]
        sql2 = f'''SELECT distinct(A.`转债代码`),A.`转债名称` FROM `stock`.`kezhuanzhai_all` as A,`stock`.`stockBasicInfo` AS B where A.`正股名称`=B.`股票简称` and `日期`>'{last5Days}' and B.`所属概念` like '%{gaiNian}%';'''
        results2, _ = dbConnection.Query(sql2)
        data.append((date,gaiNian,results2))
    
    title = f'''{last5Days} - {tradingDays[-1]} 近5个交易日新增加概念总结:'''
    msg = FormatCardOfNewGaiNian5Days(data,title)
    content = json.dumps(msg,ensure_ascii=False)
    msg_type = "interactive"
    sendMessageByWebhook(webhook,secret,msg_type,content)


def SendReDianOfToday(dbConnection,tradingDays,webhook,secret):
    # 今日热点概念相关可转债
    today = tradingDays[-1]
    sql = f'''SELECT * FROM stock.rediandaily where `日期` = "{today}";'''
    results, _ = dbConnection.Query(sql)
    if len(results) == 0:
        return
    
    data = {}
    for result in results:
        redians = result[1].split(';')
        for redian in redians:
            sql2 = f'''SELECT distinct(A.`转债代码`),A.`转债名称`, A.`现价` FROM `stock`.`kezhuanzhai` as A,`stock`.`stockBasicInfo` AS B where A.`正股名称`=B.`股票简称` and A.`日期` = "{today}" and B.`所属概念` like '%{redian}%';'''
            results2, _ = dbConnection.Query(sql2)
            for r2 in results2:
                if r2[0] not in data:
                    data[r2[0]] = [r2[1], redian,r2[2]]
                else:
                    data[r2[0]][1] = data[r2[0]][1] + ";" + redian

    newData = sorted(data.items(),key=lambda x:x[1][2],reverse=False)
    title = ["**转债代码**","**转债名称**","**概念**"]
    msg = FormatCardOfReDianToday(today,results[0][1],newData,title)
    content = json.dumps(msg,ensure_ascii=False)
    msg_type = "interactive"
    sendMessageByWebhook(webhook,secret,msg_type,content)


def SendNDaysKeZhuanZaiQiangShu(dbConnection,tradingDays,webhook,secret,days = -5):
    #近5日出现有强赎公告的
    last5Days = tradingDays[days]
    sql = f'''SELECT `日期`,`转债代码`, `转债名称`, `现价`, `提示` FROM stock.kezhuanzhai_all where `提示` !="" and `日期` >= "{last5Days}";'''
    results, _ = dbConnection.Query(sql)
    if len(results) == 0:
        return
    
    data = {}
    exceptions = ["不提前赎回","合格机构投资者可买","不行使提前赎回权利"]
    for result in results:
        date = result[0]
        id = result[1]
        name = result[2]
        price = result[3]
        tips = result[4].strip().split('\n')[0].split('：')[0]
        flag = False
        for exception in exceptions:
            if tips.find(exception)!= -1:
                flag = True
                break
        
        if flag == True:
            continue
        if id not in data:
            data[id] = {}
        
        if "公告" not in data[id]:
            data[id]["公告"] = []

        if tips not in data[id]["公告"]:
            data[id]["公告"].append(tips)
        
        data[id]["现价"] = price
        if "转债名称" not in data[id]:
            data[id]["转债名称"] = name

    print(data)
    
    title = f'''{last5Days} - {tradingDays[-1]} 近{-days}个交易日提示性公告:'''
    msg = FormatCardOfQiangShu(data,title)
    content = json.dumps(msg,ensure_ascii=False)
    msg_type = "interactive"
    sendMessageByWebhook(webhook,secret,msg_type,content)


def SendNewStocks(dbConnection,tradingDays,webhook,secret):
    # 新股发行通知
    sql = f'''SELECT `股票代码`,`股票名称`,`申购日` FROM stock.newstocks where `申购日期` >="{tradingDays[-1]}" order by `申购日期` ASC;'''
    results, _ = dbConnection.Query(sql)
    if len(results) == 0:
        return
    
    data = []
    for result in results:
        stockID = result[0]
        stockName = result[1]
        date = result[2]
        ban = "上交所"
        if re.match('^00.*',stockID) is not None:
            ban = "中小板"
        elif re.match('^30.*',stockID) is not None:
            ban = "创业板"
        elif re.match('^60.*',stockID) is not None:
            ban = "主板"
        elif re.match('^68.*',stockID) is not None:
            ban = "科创板"
        elif re.match('^8.*|^920.*',stockID) is not None:
            ban = "北交所"
            data.append((stockName,date,ban))  
        else:
            ban = "未知"
       
    
    title = f'''新股打新日历{tradingDays[-1]}:'''
    msg = FormatCardOfNewStock(data,title)
    if msg is None:
        return
    content = json.dumps(msg,ensure_ascii=False)
    msg_type = "interactive"
    sendMessageByWebhook(webhook,secret,msg_type,content)

def SendkeZhuanZaiScore(dbConnection,date,diDianDate,webhook,secret,limit = 50):
    sql = f'''select A.`转债代码`,B.`转债名称`,A.`总分`,A.`抗跌分数周期`,A.`领涨分数周期` from kezhuanzai_score As A,kezhuanzhai AS B where A.`日期` = "{date}" and B.`日期` = "{diDianDate}"  and A.`转债代码` = B.`转债代码` order by A.`总分` DESC limit {limit};'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    title = ["**序号**","**代码**","**名称**","**分数**"]
    msg = FormatCardOfKeZhuanZaiScore(date,df,title,diDianDate,limit)
    
    content = json.dumps(msg,ensure_ascii=False)
    #print(content)
    msg_type = "interactive"
    sendMessageByWebhook(webhook,secret,msg_type,content)


def SendZhuanZaiPingJiChanged(dbConnection,tradingDays,webhook,secret):
    sql = f'''SELECT A.`日期`, A.`转债代码`,A.`转债名称`, A.`评级`,B.`评级` As '昨日评级' FROM stock.kezhuanzhai_all As A, (SELECT * FROM stock.kezhuanzhai_all where `日期`= "{tradingDays[-2]}") AS B where A.`日期`= "{tradingDays[-1]}" and A.`转债代码` = B.`转债代码` and A.`评级` != B.`评级`;'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    if df.empty:
        return
    
    title = ["**代码**","**名称**","**昨日评级**","**今日评级**"]
    msg = FormatCardOfKeZhuanZaiPingJiChanged(tradingDays[-1],df,title)
    content = json.dumps(msg,ensure_ascii=False)
    #print(content)
    msg_type = "interactive"
    sendMessageByWebhook(webhook,secret,msg_type,content)