from message.feishu.webhook_api import sendMessageByWebhook
from message.feishu.messageformat_feishu import FormatCardOfZhuanZaiYuJing,FormatCardOfNewGaiNian
import pandas as pd
import json

def SendKeZhuanZaiYuJing(dbConnection,tradingDays,webhook,secret):
    # 可转债不符合条件预警
    sql = f'''select `转债代码`,`转债名称`,`筛选结果`as `原因`  FROM stock.kezhuanzhai_all where `日期` = "{tradingDays[-1]}" and `转债代码` in (select `转债代码` FROM stock.kezhuanzhai where `日期` = "{tradingDays[-2]}" and  `转债代码` not in (select `转债代码` FROM stock.kezhuanzhai where `日期` = "{tradingDays[-1]}"))'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    if df.empty:
        return

    t = FormatCardOfZhuanZaiYuJing(tradingDays[-1],df)
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

        sql2 = f'''SELECT A.`转债代码`,A.`转债名称` FROM `stock`.`kezhuanzhai` as A,`stock`.`stockBasicInfo` AS B where A.`正股名称`=B.`股票简称` and `日期`='{tradingDays[-1]}' and B.`所属概念` like '%{gainian}%' order by `PB` DESC;'''
        results2, _ = dbConnection.Query(sql2)
        msg = FormatCardOfNewGaiNian(tradingDays[-1],gainian,results2,["**转债代码**","**转债名称**"])
        content = json.dumps(msg,ensure_ascii=False)
        msg_type = "interactive"
        sendMessageByWebhook(webhook,secret,msg_type,content)
        
        s = ";".join([f'''{t[0]} {t[1]}''' for t in results2])
        sql3 = f'''UPDATE `stock`.`stockgainiannew` SET `可转债` = '{s}' WHERE (`日期` = '{tradingDays[-1]}') and (`新概念` = '{gainian}');'''
        dbConnection.Execute(sql3)