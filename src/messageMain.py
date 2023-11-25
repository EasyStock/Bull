from message.message_feishu import authorize_tenant_access_token,sendMessage
from mysql.connect2DB import ConnectToDB
from DBOperating import GetTradingDateLastN
import pandas as pd
import json

receive_id_type = "chat_id"
receive_id = "oc_5686c403b41394ceda73e3c7e2f46259" # 调试ID
receive_id = "oc_688ec551a1df49de78d6c5bf5b623792" # 致富专区群



def formatCardOfFeishu(date,df):
    if df.empty:
        return None
    contents = []
    tag = {"tag":"hr"}
    for _, row in df.iterrows():
        reason = row["原因"].replace("\t"," ").replace("\u3000"," ")
        stockID = row["转债代码"]
        stockName = row["转债名称"]
        reason = f'''**<font color=red>{reason}</font>**'''
        s = f"**转债名称** : {stockName}\n**转债代码** : {stockID}\n**原        因** : {reason}"
        content = {"content":s,"tag":"markdown"}
        contents.append(content)
        contents.append(tag)
    t = f"可转债条件不符合条件预警:{date}"
    title = {"content":t,"tag":"plain_text"}
    ret = {"config":{"wide_screen_mode":True},"elements":contents, "header":{"template":"red","title":title}}
    return ret

def formatCardOfNewGaiNian(date,gainian, stocks,kezhuanzais):
    elements = []
    tag = {"tag":"hr"}
    stockHead = {"tag":"column_set","flex_mode":"none","background_style":"grey","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":"**股票代码**","text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":"**股票名称**","text_align":"center"}]}]}
    kezhuanzaiHeadHead = {"tag":"column_set","flex_mode":"none","background_style":"grey","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":"**转债代码**","text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":"**转债名称**","text_align":"center"}]}]}
    elements.append(tag)
    elements.append(stockHead)

    for stock in stocks:
        stockID = f'''{stock[0]}'''
        stockName = f'''{stock[1]}'''
        line = {"tag":"column_set","flex_mode":"none","background_style":"default","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockID,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockName,"text_align":"center"}]}],"horizontal_spacing":"small"}
        elements.append(line)

    elements.append(tag)
    elements.append(kezhuanzaiHeadHead)
    for zhuanzai in kezhuanzais:
        zhuanzaiID = f'''{zhuanzai[0]}'''
        zhuanzaiName = f'''{zhuanzai[1]}'''
        line = line = {"tag":"column_set","flex_mode":"none","background_style":"default","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":zhuanzaiID,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":zhuanzaiName,"text_align":"center"}]}],"horizontal_spacing":"small"}
        elements.append(line)

    title = f"{date} 新增概念:{gainian}"
    return {"config":{"wide_screen_mode":True},"elements":elements,"header":{"template":"turquoise","title":{"content":title,"tag":"plain_text"}}}


def SendKeZhuanZaiInfo(dbConnection,tradingDays):
    sql = f'''select `转债代码`,`转债名称`,`筛选结果`as `原因`  FROM stock.kezhuanzhai_all where `日期` = "{tradingDays[-1]}" and `转债代码` in (select `转债代码` FROM stock.kezhuanzhai where `日期` = "{tradingDays[-2]}" and  `转债代码` not in (select `转债代码` FROM stock.kezhuanzhai where `日期` = "{tradingDays[-1]}"))'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    if df.empty:
        return

    t = formatCardOfFeishu(tradingDays[-1],df)
    content = json.dumps(t,ensure_ascii=False)
    msg_type = "interactive"

    _tenant_access_token = authorize_tenant_access_token()
    sendMessage(_tenant_access_token,receive_id_type,receive_id,msg_type,content)


def NewGaiNian(dbConnection,tradingDays):
    sql = f'''SELECT * FROM stock.gainian where `概念名称` not in (SELECT `概念名称` FROM stock.gainian where `更新日期`="{tradingDays[-2]}") and `更新日期`="{tradingDays[-1]}";'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    if df.empty:
        return

    _tenant_access_token = authorize_tenant_access_token()

    for result in results:
        gainian = result[0]
        sql1 = f'''SELECT `股票代码`, `股票简称` FROM stock.stockbasicinfo where `所属概念` like "%{gainian}%";'''
        results1, _ = dbConnection.Query(sql1)
        sql2 = f'''SELECT A.`转债代码`,A.`转债名称` FROM `stock`.`kezhuanzhai` as A,`stock`.`stockBasicInfo` AS B where A.`正股名称`=B.`股票简称` and `日期`='{tradingDays[-1]}' and B.`所属概念` like '%{gainian}%' order by `PB` DESC;'''
        results2, _ = dbConnection.Query(sql2)

        msg = formatCardOfNewGaiNian(tradingDays[-1],gainian,results1,results2)
        content = json.dumps(msg,ensure_ascii=False)
        msg_type = "interactive"
        sendMessage(_tenant_access_token,receive_id_type,receive_id,msg_type,content)
        

if __name__ == "__main__":
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,5)
    SendKeZhuanZaiInfo(dbConnection,tradingDays)
    NewGaiNian(dbConnection,tradingDays)