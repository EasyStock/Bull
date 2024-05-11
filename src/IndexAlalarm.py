from mysql.connect2DB import ConnectToDB
import pandas as pd
from MA.MAMgr import CMAMgr
from MA.MACross import CMACross
import numpy as np
from message.feishu.messageformat_feishu import FormatCardOfAlarm
from message.feishu.webhook_api import sendMessageByWebhook
import json

def ChuangYeBanAlarm():
    dbConnection = ConnectToDB()
    sql = f'''SELECT * FROM stock.kaipanla_index where `StockID` = "SZ399006";'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    df.set_index("date",drop=True,inplace=True)
    mgr = CMAMgr()
    messages = []
    res1 = mgr.predict(df,"last_px",(5,10,20,30,60))
    res2 = mgr.Cross(df,"last_px",5,10)
    messages.extend(res1)
    messages.extend(res2)
    t = FormatCardOfAlarm("",messages)
    content = json.dumps(t,ensure_ascii=False)
    msg_type = "interactive"
    webhook = "https://open.feishu.cn/open-apis/bot/v2/hook/4901573e-b858-434a-a787-5faa28982b1a"
    secret = "brYyzPbSks4OKnMgdwKvIh"
    sendMessageByWebhook(webhook,secret,msg_type,content)

    df["MA5"] = df["last_px"].rolling(window=5).mean()
    df["MA10"] = df["last_px"].rolling(window=10).mean()
    df["MA20"] = df["last_px"].rolling(window=20).mean()
    df["MA30"] = df["last_px"].rolling(window=30).mean()
    df["MA60"] = df["last_px"].rolling(window=60).mean()
    df["MA120"] = df["last_px"].rolling(window=120).mean()
    df["MA200"] = df["last_px"].rolling(window=200).mean()
    df.to_excel("/tmp/创业板.xlsx",index=True)

if __name__ == "__main__":
    ChuangYeBanAlarm()