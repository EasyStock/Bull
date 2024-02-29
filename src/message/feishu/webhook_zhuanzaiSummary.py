from message.feishu.webhook_api import sendMessageByWebhook
from message.feishu.messageformat_feishu import ForamtKeZhuanZaiSummary,ForamtKeZhuanZaiSummary_PingJi,ForamtKeZhuanZaiSummary_NewStock, \
                                                ForamtKeZhuanZaiSummary_qiangsu,ForamtKeZhuanZaiSummary_NewGaiNian5Days,ForamtKeZhuanZaiSummary_PingJiBuFu, \
                                                ForamtKeZhuanZaiSummary_YouXiFuZhai,ForamtKeZhuanZaiSummary_shengyuGuiMo,ForamtKeZhuanZaiSummary_ST, \
                                                ForamtKeZhuanZaiSummary_ReDianToday
import pandas as pd
import json
import re

def PingJi(dbConnection,tradingDays):
    # 评级变动
    sql = f'''SELECT A.`日期`, A.`转债代码`,A.`转债名称`, A.`评级`,B.`评级` As '昨日评级' FROM stock.kezhuanzhai_all As A, (SELECT * FROM stock.kezhuanzhai_all where `日期`= "{tradingDays[-2]}") AS B where A.`日期`= "{tradingDays[-1]}" and A.`转债代码` = B.`转债代码` and A.`评级` != B.`评级`;'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    title = ["**代码**","**名称**","**昨日评级**","**今日评级**"]
    pingJi = ForamtKeZhuanZaiSummary_PingJi(df,title)
    return pingJi
    # 评级变动

def PingJiBuFu(dbConnection,tradingDays):
    # 近5日评级不符合
    last5Days = tradingDays[-5]
    last6Days = tradingDays[-6]
    sql = f'''SELECT  distinct(`转债代码`),`转债名称`, `评级` FROM stock.kezhuanzhai_all where `日期` >= "{last5Days}" and `评级` not in ("AAA","AA+","AA","AA-","A+") and `转债代码` not in (SELECT `转债代码` FROM stock.kezhuanzhai_all where `日期` = "{last6Days}" and `评级` not in ("AAA","AA+","AA","AA-","A+"));'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    title = ["**代码**","**名称**","**评级**"]
    pingJi = ForamtKeZhuanZaiSummary_PingJiBuFu(df,title)
    return pingJi
    # 评级不符合

def YouxiFuZhaiLv(dbConnection,tradingDays):
    # 近5日有息负债率大于70
    last5Days = tradingDays[-5]
    last6Days = tradingDays[-6]
    sql = f'''SELECT distinct(`转债代码`),`转债名称`,`有息负债率` FROM stock.kezhuanzhai_all where `日期` >= "{last5Days}" and `有息负债率` >=70 and `转债代码` not in (SELECT `转债代码` FROM stock.kezhuanzhai_all where `日期` = "{last6Days}" and `有息负债率` >= 70 );'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    title = ["**代码**","**名称**","**有息负债率**"]
    pingJi = ForamtKeZhuanZaiSummary_YouXiFuZhai(df,title)
    return pingJi
    # 近5日有息负债率大于70

def ShengYuGuiMo(dbConnection,tradingDays):
    # 近5日剩余规模小于3.5
    last5Days = tradingDays[-5]
    last6Days = tradingDays[-6]
    sql = f'''SELECT distinct(`转债代码`),`转债名称`,`剩余规模` FROM stock.kezhuanzhai_all where `日期` >= "{last5Days}" and  `剩余规模` <3.5 and `转债代码` not in (SELECT `转债代码` FROM stock.kezhuanzhai_all where `日期` = "{last6Days}" and `剩余规模` <3.5 );'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    df_unique = df.drop_duplicates(subset=['转债代码', ],keep='first')
    title = ["**代码**","**名称**","**剩余规模**"]
    pingJi = ForamtKeZhuanZaiSummary_shengyuGuiMo(df_unique,title)
    return pingJi
    # 近5日剩余规模小于3.5

def StockNameST(dbConnection,tradingDays):
    # 正股被ST
    last5Days = tradingDays[-5]
    last6Days = tradingDays[-6]
    sql = f'''SELECT distinct(`转债代码`),`转债名称`,`正股名称` FROM stock.kezhuanzhai_all where `日期` >= "{last5Days}" and `正股名称` like "%ST%" and `转债代码` not in (SELECT `转债代码` FROM stock.kezhuanzhai_all where `日期` = "{last6Days}" and `正股名称` like "%ST%" );'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    title = ["**代码**","**名称**","**正股名称**"]
    pingJi = ForamtKeZhuanZaiSummary_ST(df,title)
    return pingJi
    # 正股被ST

def NewStock(dbConnection,tradingDays):
    #打新
    sql = f'''SELECT `股票代码`,`股票名称`,`申购日` FROM stock.newstocks where `申购日期` >="{tradingDays[-1]}" order by `申购日期` ASC;'''
    results, _ = dbConnection.Query(sql)
    if len(results) == 0:
        return []
    
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
        elif re.match('^8.*',stockID) is not None:
            ban = "北交所"
            data.append((stockName,date,ban))
        else:
            ban = "未知"

    daxin = ForamtKeZhuanZaiSummary_NewStock(data)
    return daxin
    #打新

def GetNewGaiNians(dbConnection,tradingDays):
    sql = f'''SELECT * FROM stock.gainian where `概念名称` not in (SELECT `概念名称` FROM stock.gainian where `更新日期`="{tradingDays[-2]}") and `更新日期`="{tradingDays[-1]}";'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    if df.empty:
        return []
    
    ret = []
    for result in results:
        gainian = result[0]
        ret.append(gainian)
        sql1 = f'''INSERT IGNORE INTO `stock`.`stockgainiannew` (`日期`, `新概念`) VALUES ('{tradingDays[-1]}', '{gainian}');'''
        dbConnection.Execute(sql1)
    return ret

def NewGaiNian5Days(dbConnection,tradingDays):
    #近5日新概念
    GetNewGaiNians(dbConnection,tradingDays)
    last5Days = tradingDays[-5]
    sql = f'''SELECT `日期`,`新概念`  FROM stock.stockgainiannew where `日期` >= "{last5Days}";'''
    results, _ = dbConnection.Query(sql)
    if len(results) == 0:
        return []
    
    data = []
    for result in results:
        date = result[0]
        gaiNian = result[1]
        sql2 = f'''SELECT distinct(A.`转债代码`),A.`转债名称` FROM `stock`.`kezhuanzhai_all` as A,`stock`.`stockBasicInfo` AS B where A.`正股名称`=B.`股票简称` and `日期`>'{last5Days}' and B.`所属概念` like '%{gaiNian}%';'''
        results2, _ = dbConnection.Query(sql2)
        data.append((date,gaiNian,results2))
    
    gaiNian = ForamtKeZhuanZaiSummary_NewGaiNian5Days(data)
    return gaiNian

    #近5日新概念

def QiangShu(dbConnection,tradingDays):
    #近5日出现有强赎公告的
    last5Days = tradingDays[-5]
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

    qiangshu = ForamtKeZhuanZaiSummary_qiangsu(data)
    return qiangshu


def ReDianOfToday(dbConnection,tradingDays):
    # 今日热点概念相关可转债
    today = tradingDays[-2]
    sql = f'''SELECT * FROM stock.rediandaily where `日期` = "{today}";'''
    results, _ = dbConnection.Query(sql)
    if len(results) == 0:
        return
    
    data = {}
    for result in results:
        redians = result[1].split(';')
        for redian in redians:
            sql2 = f'''SELECT distinct(A.`转债代码`),A.`转债名称`, A.`现价` FROM `stock`.`kezhuanzhai` as A,`stock`.`stockBasicInfo` AS B where A.`正股名称`=B.`股票简称` and A.`日期` = "{today}" and B.`所属概念` like '%{redian}%';'''
            results2, columns = dbConnection.Query(sql2)
            data[redian] = pd.DataFrame(results2,columns = columns)

    redian = ForamtKeZhuanZaiSummary_ReDianToday(data)
    return redian




def ZhuanZaiSummary(dbConnection,tradingDays,webhook,secret):
    datas = []
    redian = ReDianOfToday(dbConnection,tradingDays)
    if len(redian) > 0:
        datas.append(redian)

    daxin = NewStock(dbConnection,tradingDays)
    if len(daxin) > 0:
        datas.append(daxin)

    qiangshu = QiangShu(dbConnection,tradingDays)
    if len(qiangshu) > 0:
        datas.append(qiangshu)

    pingJi = PingJi(dbConnection,tradingDays)
    if len(pingJi) > 0:
        datas.append(pingJi)

    gaiNian5 = NewGaiNian5Days(dbConnection,tradingDays)
    if len(gaiNian5) > 0:
        datas.append(gaiNian5)

    pingJi5buFu = PingJiBuFu(dbConnection,tradingDays)
    if len(pingJi5buFu) > 0:
        datas.append(pingJi5buFu)

    fuzhaiLv = YouxiFuZhaiLv(dbConnection,tradingDays)
    if len(fuzhaiLv) > 0:
        datas.append(fuzhaiLv)


    guimo = ShengYuGuiMo(dbConnection,tradingDays)
    if len(guimo) > 0:
        datas.append(guimo)

    st = StockNameST(dbConnection,tradingDays)
    if len(st) > 0:
        datas.append(st)

    msg = ForamtKeZhuanZaiSummary(datas,tradingDays[-1])
    content = json.dumps(msg,ensure_ascii=False)
    msg_type = "interactive"
    sendMessageByWebhook(webhook,secret,msg_type,content)