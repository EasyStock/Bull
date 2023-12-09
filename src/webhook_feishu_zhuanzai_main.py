from message.feishu.webhook_zhuanzai import SendKeZhuanZaiYuJing,SendKeZhuanZaiNewGaiNian,Send5DaysKeZhuanZaiNewGaiNian,SendNDaysKeZhuanZaiQiangShu
from mysql.connect2DB import ConnectToDB
from DBOperating import GetTradingDateLastN
from time import sleep

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    helpStr = f'''
    -w : webhook
    -s : secret
    -0 : options
    '''
    parser.add_argument('-w','--webhook', action="store",default=True,help="Webhook URL")
    parser.add_argument('-s','--secret', action="store",default=True,help="secret")
    parser.add_argument('-o','--options', action="store",default=True,nargs="+",type=int, help="1. 可转债预警, 2. 当日新概念提示 3. 5日新概念提示")
    args = parser.parse_args()
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,5)
    # args.webhook = "https://open.feishu.cn/open-apis/bot/v2/hook/4901573e-b858-434a-a787-5faa28982b1a"
    # args.secret = "brYyzPbSks4OKnMgdwKvIh"
    # args.options = [1,2,3,4]
    if args.webhook and args.secret:
        if 1 in args.options:
            SendKeZhuanZaiYuJing(dbConnection,tradingDays,args.webhook,args.secret)

        if 2 in args.options:
            SendKeZhuanZaiNewGaiNian(dbConnection,tradingDays,args.webhook,args.secret)

        if 3 in args.options:
            Send5DaysKeZhuanZaiNewGaiNian(dbConnection,tradingDays,args.webhook,args.secret)

        if 4 in args.options:
            SendNDaysKeZhuanZaiQiangShu(dbConnection,tradingDays,args.webhook,args.secret,-5)