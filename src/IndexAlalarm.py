from mysql.connect2DB import ConnectToDB
from DBOperating import GetTradingDateLastN
import pandas as pd
from MA.MAMgr import CMAMgr
from MA.MAManager import CMAManager

from message.feishu.messageformat_feishu import FormatCardOfAlarm
from message.feishu.webhook_api import sendMessageByWebhook
import json

def IndexAlarm():
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,3)
    mgr = CMAManager(dbConnection)
    messages = mgr.IndexInfo()
    t = FormatCardOfAlarm(tradingDays[-1],messages)
    content = json.dumps(t,ensure_ascii=False)
    msg_type = "interactive"
    webhook = "https://open.feishu.cn/open-apis/bot/v2/hook/4901573e-b858-434a-a787-5faa28982b1a"
    secret = "brYyzPbSks4OKnMgdwKvIh"
    sendMessageByWebhook(webhook,secret,msg_type,content)

if __name__ == "__main__":
    IndexAlarm()