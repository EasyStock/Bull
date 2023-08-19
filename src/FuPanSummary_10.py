from mysql.connect2DB import ConnectToDB
from fupan.tradingDate import GetTradingDateLastN
from fupan.fupanSummary import Summary
from fupan.fupanSummary2 import CFupanSummary



def WriteSummary():
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,70)
    fupanSummary = CFupanSummary(dbConnection,tradingDays[-1])
    fupanSummary.WirteFupanSummary()


def GetSummary():
    dbConnection = ConnectToDB()
    fupanSummary = CFupanSummary(dbConnection,None)
    fupanSummary.GetFuPanLastSummary()

if __name__ == "__main__":
    #WriteSummary()
    #GetSummary()
    import argparse
    parser = argparse.ArgumentParser()
    helpStr = f'''
    -w : 写入复盘记录 
    -r : 读取复盘记录
    '''
    parser.add_argument('-r','--read', action="store_true",default=False,help="读取复盘记录")
    parser.add_argument('-w','--write', action="store_true",default=True,help="写入复盘记录")
    args = parser.parse_args()
    if args.read:
        GetSummary()
    elif args.write:
        WriteSummary()