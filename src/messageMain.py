from message.message_feishu import authorize_tenant_access_token,sendMessage
from mysql.connect2DB import ConnectToDB
from DBOperating import GetTradingDateLastN
import pandas as pd
import json

receive_id_type = "chat_id"
debug_receive_id = "oc_5686c403b41394ceda73e3c7e2f46259" # 调试ID
kezhuanzai_receive_id = "oc_688ec551a1df49de78d6c5bf5b623792" # 致富专区群 可转债群
meirifupan_receive_id = "oc_43e45c71e0b3fb59c6e66129403a46b4" # 每日复盘群 股票群



_tenant_access_token = authorize_tenant_access_token()

def _formatCardOfFeishu(date,df):
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

def _formatCardOfNewGaiNian(date,gainian, stocks,titles):
    elements = []
    tag = {"tag":"hr"}
    stockHead = {"tag":"column_set","flex_mode":"none","background_style":"grey","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[0],"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[1],"text_align":"center"}]}]}
    elements.append(tag)
    elements.append(stockHead)

    for stock in stocks:
        stockID = f'''{stock[0]}'''
        stockName = f'''{stock[1]}'''
        line = {"tag":"column_set","flex_mode":"none","background_style":"default","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockID,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockName,"text_align":"center"}]}],"horizontal_spacing":"small"}
        elements.append(line)

    title = f"{date} 新增概念:{gainian}"
    return {"config":{"wide_screen_mode":True},"elements":elements,"header":{"template":"turquoise","title":{"content":title,"tag":"plain_text"}}}

def _markdownFontColor(data,color = "red"):
    return f'''**<font color='{color}'>{data}</font>**'''

def _markdownFontHighlight(data):
    return f'''**{data}**'''

def _markdown(data):
    tag = {"tag":"markdown","content":data}
    return tag

def _formatCardOfMeiRiFuPan(date,df):
    elements = []
    tag = {"tag":"hr"}
    line1 = f'''{_markdownFontHighlight("1. 市场总体情况")}\n a.今日两市成交量:{_markdownFontColor("8133亿")},量增:{_markdownFontColor("-109亿")}\n b.红盘:{_markdownFontColor("1111")}, 绿盘: {_markdownFontColor("1111")}\n c.涨停: {_markdownFontColor("31")}, 跌停: {_markdownFontColor("12")}\n 炸板: {_markdownFontColor("14")}，炸板率: {_markdownFontColor("31.1%")}\n 连板个数: <font color='red'>16</font>，近30个交易日最多连板个数: <font color='red'>26</font> 最少连板个数: <font color='red'>6</font>'''
    elements.append(_markdown(line1))
    elements.append(tag)
    elements.append(_markdown("2. 今日10CM首板奖励率:53.85%，20CM首板奖励率:50.00%，10CM连板奖励率:60.00%，20CM连板奖励率:-%,2连板个股(9个),3连板个股(4个): 四环生物;龙版传媒;凯华材料;惠威科技， 4连及以上个股(3个)：九鼎投资4;志晟信息4;伟时电子4,近30个交易日高度板: 14 板"))
    elements.append(tag)
    elements.append(_markdown("3. **今日势能**:-2, **动能**:0, 连板股的红盘比:0.6, 首板红盘比:0.54, 备注:无\n高潮: 动能综合值=12 且 势能综合值=10 或者 连板股的红盘比 >=0.78 首板股的红盘比 >=0.78\n半高潮: 只有 连板股的红盘比 >=0.78\n冰点期判断 - 强势行情: 如果动能综合值 =-12 且 势能综合值 <=-2 或者 (动能综合值<=-8 且 势能综合值<=-2) 出现两次\n冰点期判断 - 弱势行情: 如果动能综合值 <=-8 且 势能综合值 =-10 且首板赚钱效应和连板赚钱效应都出现过 <0.4 或者 连续两天动能综合值和势能综合值都<=-6"))
    elements.append(tag)
    elements.append(_markdown("**今日热点板块:** 次新 和 次新股"))
    title = f" 每日复盘 - {date}"
    return {"config":{"wide_screen_mode":True},"elements":elements,"header":{"template":"red","title":{"content":title,"tag":"plain_text"}}}


def SendKeZhuanZaiYuJing(dbConnection,tradingDays,receive_id=debug_receive_id):
    sql = f'''select `转债代码`,`转债名称`,`筛选结果`as `原因`  FROM stock.kezhuanzhai_all where `日期` = "{tradingDays[-1]}" and `转债代码` in (select `转债代码` FROM stock.kezhuanzhai where `日期` = "{tradingDays[-2]}" and  `转债代码` not in (select `转债代码` FROM stock.kezhuanzhai where `日期` = "{tradingDays[-1]}"))'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    if df.empty:
        return

    t = _formatCardOfFeishu(tradingDays[-1],df)
    content = json.dumps(t,ensure_ascii=False)
    msg_type = "interactive"

    sendMessage(_tenant_access_token,receive_id_type,receive_id,msg_type,content)


def NewGaiNian_zhuanzai(dbConnection,tradingDays,receive_id=debug_receive_id):
    sql = f'''SELECT * FROM stock.gainian where `概念名称` not in (SELECT `概念名称` FROM stock.gainian where `更新日期`="{tradingDays[-2]}") and `更新日期`="{tradingDays[-1]}";'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    if df.empty:
        return

    for result in results:
        gainian = result[0]

        sql2 = f'''SELECT A.`转债代码`,A.`转债名称` FROM `stock`.`kezhuanzhai` as A,`stock`.`stockBasicInfo` AS B where A.`正股名称`=B.`股票简称` and `日期`='{tradingDays[-1]}' and B.`所属概念` like '%{gainian}%' order by `PB` DESC;'''
        results2, _ = dbConnection.Query(sql2)

        msg = _formatCardOfNewGaiNian(tradingDays[-1],gainian,results2,["**转债代码**","**转债名称**"])
        content = json.dumps(msg,ensure_ascii=False)
        msg_type = "interactive"
        sendMessage(_tenant_access_token,receive_id_type,receive_id,msg_type,content)

def NewGaiNian_Stock(dbConnection,tradingDays,receive_id=debug_receive_id):
    sql = f'''SELECT * FROM stock.gainian where `概念名称` not in (SELECT `概念名称` FROM stock.gainian where `更新日期`="{tradingDays[-2]}") and `更新日期`="{tradingDays[-1]}";'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    if df.empty:
        return

    for result in results:
        gainian = result[0]
        sql1 = f'''SELECT `股票代码`, `股票简称` FROM stock.stockbasicinfo where `所属概念` like "%{gainian}%";'''
        results1, _ = dbConnection.Query(sql1)


        msg = _formatCardOfNewGaiNian(tradingDays[-1],gainian,results1,["**股票代码**","**股票简称**"])
        content = json.dumps(msg,ensure_ascii=False)
        msg_type = "interactive"
        sendMessage(_tenant_access_token,receive_id_type,receive_id,msg_type,content)


def MeiRiFuPan_Stock(dbConnection,tradingDays,receive_id=debug_receive_id):
    sql = f'''SELECT * FROM stock.fuPan where `日期` = "{tradingDays[-1]}" ;'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    if df.empty:
        return
    msg = _formatCardOfMeiRiFuPan(tradingDays[-1],["**股票代码**","**股票简称**"])
    content = json.dumps(msg,ensure_ascii=False)
    print(content)
    msg_type = "interactive"
    sendMessage(_tenant_access_token,receive_id_type,receive_id,msg_type,content)

def SendKeZhuanZaiInfo(dbConnection,tradingDays):
    receive_id = kezhuanzai_receive_id
    #receive_id = debug_receive_id
    SendKeZhuanZaiYuJing(dbConnection,tradingDays,receive_id)
    NewGaiNian_zhuanzai(dbConnection,tradingDays,receive_id)

def SendStockInfo(dbConnection,tradingDays):
    receive_id = meirifupan_receive_id
    receive_id = debug_receive_id
    
    # NewGaiNian_Stock(dbConnection,tradingDays,receive_id)
    MeiRiFuPan_Stock(dbConnection,tradingDays,receive_id)

if __name__ == "__main__":
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,5)
    #SendKeZhuanZaiInfo(dbConnection,tradingDays)
    SendStockInfo(dbConnection,tradingDays)