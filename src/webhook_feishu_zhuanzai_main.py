from message.feishu.webhook_zhuanzai import SendKeZhuanZaiYuJing,SendKeZhuanZaiNewGaiNian
from mysql.connect2DB import ConnectToDB
from DBOperating import GetTradingDateLastN
from time import sleep

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
    if args.webhook and args.secret:
        SendKeZhuanZaiYuJing(dbConnection,tradingDays,args.webhook,args.secret)
        sleep(3)
        SendKeZhuanZaiNewGaiNian(dbConnection,tradingDays,args.webhook,args.secret)