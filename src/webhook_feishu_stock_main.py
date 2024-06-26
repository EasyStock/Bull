from message.feishu.webhook_stock import SendNewGaiNianOfStock,SendMeiRiFuPan_Stock
from mysql.connect2DB import ConnectToDB
from DBOperating import GetTradingDateLastN
from time import sleep
import datetime
import pytz

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    helpStr = f'''
    -w : webhook
    -s : secret
    '''
    parser.add_argument('-w','--webhook', action="store",default=True,help="Webhook URL")
    parser.add_argument('-s','--secret', action="store",default=True,help="secret")
    args = parser.parse_args()
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,5)
    # args.webhook = "https://open.feishu.cn/open-apis/bot/v2/hook/4901573e-b858-434a-a787-5faa28982b1a"
    # args.secret = "brYyzPbSks4OKnMgdwKvIh"

    today = datetime.datetime.now(pytz.timezone('Asia/Shanghai')).date()
    print("现在UTC时间是:",datetime.datetime.utcnow())
    if tradingDays[-1] == str(today) and datetime.datetime.utcnow().time() < datetime.time(9, 0):
        tradingDays = tradingDays[:-1]

    if args.webhook and args.secret:
        SendNewGaiNianOfStock(dbConnection,tradingDays,args.webhook,args.secret)
        sleep(3)
        SendMeiRiFuPan_Stock(dbConnection,tradingDays,args.webhook,args.secret)
        