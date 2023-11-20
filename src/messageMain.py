from message.message_feishu import formatCardOfFeishu,authorize_tenant_access_token,sendMessage
from mysql.connect2DB import ConnectToDB
from DBOperating import GetTradingDateLastN
import pandas as pd
import json

def SendKeZhuanZaiInfo():
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,3)
    sql = f'''select `转债代码`,`转债名称`,`筛选结果`as `原因`  FROM stock.kezhuanzhai_all where `日期` = "{tradingDays[-1]}" and `转债代码` in (select `转债代码` FROM stock.kezhuanzhai where `日期` = "{tradingDays[-2]}" and  `转债代码` not in (select `转债代码` FROM stock.kezhuanzhai where `日期` = "{tradingDays[-1]}"))'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    if df.empty:
        return

    receive_id_type = "chat_id"
    receive_id = "oc_5686c403b41394ceda73e3c7e2f46259"

    t = formatCardOfFeishu(tradingDays[-1],df)
    content = json.dumps(t,ensure_ascii=False)
    msg_type = "interactive"

    _tenant_access_token = authorize_tenant_access_token()
    sendMessage(_tenant_access_token,receive_id_type,receive_id,msg_type,content)

if __name__ == "__main__":
    SendKeZhuanZaiInfo()