from mysql.connect2DB import ConnectToDB
from ifind.token import CIFindToken
from ifind.fengdan import CFengdan
from DBOperating import GetTradingDateLastN

def refreshAccessToken():
    dbConnection = ConnectToDB()
    finder = CIFindToken(dbConnection)
    finder.refreshAccessToken()

def FengDan():
    dbConnection = ConnectToDB()
    finder = CIFindToken(dbConnection)
    finder.GetTokenInfoFromDB()
    token = finder.access_token

    fengdan = CFengdan(dbConnection,token)
    #fengdan.Query(["000151.SZ"],"2023-06-16")
    tradingDays = GetTradingDateLastN(dbConnection,3)
    fengdan.ProcessFengDanLastN(tradingDays)

if __name__ == "__main__":
    refreshAccessToken()
    import argparse
    parser = argparse.ArgumentParser()
    helpStr = f'''
    0 : 刷新token 
    1 : 写入封单数据
    '''
    parser.add_argument('-i','--index',default=0,choices=[0,1],type=int, help=helpStr)
    args = parser.parse_args()

    if args.index == 0:
        refreshAccessToken()
    elif args.index == 1:
        FengDan()