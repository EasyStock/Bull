from mysql.connect2DB import ConnectToDB
from DBOperating import GetTradingDateLastN
from MA.MAManager import CMAManager

from message.feishu.messageformat_feishu import FormatCardOfAlarm
from message.feishu.webhook_api import sendMessageByWebhook
import json

def IndexAlarm(dbConnection,tradingDays,webhook,secret):
    mgr = CMAManager(dbConnection)
    messages = mgr.IndexInfoToMessages(tradingDays[-1])
    t = FormatCardOfAlarm(tradingDays[-1],messages)
    content = json.dumps(t,ensure_ascii=False)
    msg_type = "interactive"
    sendMessageByWebhook(webhook,secret,msg_type,content)

if __name__ == "__main__":
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,3)
    webhook = "https://open.feishu.cn/open-apis/bot/v2/hook/4901573e-b858-434a-a787-5faa28982b1a"
    secret = "brYyzPbSks4OKnMgdwKvIh"
    IndexAlarm(dbConnection,tradingDays,webhook,secret)