
import datetime


def GetTradingDateLastN(dbConnection,N):
    today = datetime.date.today()
    end = today.strftime("%Y-%m-%d")
    sql = f"SELECT `日期` FROM stock.treadingDay where `开市` =1 and `交易所`='SSE' and `日期`<='{end}'"
    print(sql)
    res,_ = dbConnection.Query(sql)
    results = [r[0] for r in res]
    return results[-N:]