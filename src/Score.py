from mysql.connect2DB import ConnectToDB
from score.scoreMgr import CScoreMgr
from DBOperating import GetTradingDateLastN
from score.scoreZai import CScoreZaiMgr,CSelectZai
from score.scoreStock import CScoreStock

def Score1():
    dbConnection = ConnectToDB()
    # indexParam = {
    #     "抗跌分数":{
    #         "startDay":"2023-10-16",
    #         "endDay":"2023-10-23",
    #     },
    #     "领涨分数":{
    #         "startDay":"2023-10-24",
    #         "endDay":"2023-11-08",
    #     }
    # }
    # indexParam = {
    #     "抗跌分数":{
    #         "startDay":"2023-12-05",
    #         "endDay":"2023-12-20",
    #     },
    #     "领涨分数":{
    #         "startDay":"2023-12-27",
    #         "endDay":"2023-12-29",
    #     }
    # }
    # indexParam = {
    #     "抗跌分数":{
    #         "startDay":"2024-01-02",
    #         "endDay":"2024-01-17",
    #     },
    #     "领涨分数":{
    #         "startDay":"2024-01-18",
    #         "endDay":"2024-01-18",
    #     }
    # }

    # indexParam = {
    #     "抗跌分数":{
    #         "startDay":"2024-01-02",
    #         "endDay":"2024-01-23",
    #     },
    #     "领涨分数":{
    #         "startDay":"2024-01-24",
    #         "endDay":"2024-01-26",
    #     }
    # }
    
    # indexParam = {
    #     "抗跌分数":{
    #         "startDay":"2024-01-02",
    #         "endDay":"2024-02-05",
    #     },
    #     "领涨分数":{
    #         "startDay":"2024-02-06",
    #         "endDay":"2024-03-29",
    #     }
    # }

    indexParam = {
        "抗跌分数":{
            "startDay":"2024-04-02",
            "endDay":"2024-04-22",
        },
        "领涨分数":{
            "startDay":"2024-04-23",
            "endDay":"2024-05-21",
        }
    }
    mgr = CScoreMgr(dbConnection)
    mgr.Score(indexParam)
    mgr.Select(indexParam)
    # dates = ("2024-02-06","2024-02-08","2024-02-19","2024-02-20","2024-02-21","2024-02-22","2024-02-23","2024-02-26","2024-02-27","2024-02-28")
    # mgr.UnionSelect(dates)

def ScoreZaiEveryDay(dbConnection,tradingDays):
    mgr = CScoreZaiMgr(dbConnection,tradingDays[-1])
    mgr.Score()

def Score2():
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,2)
    for date in tradingDays:
        mgr = CScoreZaiMgr(dbConnection,date)
        mgr.Score()


def Select2():
    dbConnection = ConnectToDB()
    indexParam = {
        "抗跌分数":{
            "startDay":"2024-04-02",
            "endDay":"2024-04-22",
        },
        "领涨分数":{
            "startDay":"2024-04-23",
            "endDay":"2024-05-21",
        }
    }
    tradingDays = GetTradingDateLastN(dbConnection,15)
    select = CSelectZai(dbConnection,tradingDays)
    #select.Select(indexParam)
    select.SelectZhaiByScore(tradingDays[-1])


def ScoreStock():
    dbConnection = ConnectToDB()
    stock = CScoreStock(dbConnection)
    tradingDays = GetTradingDateLastN(dbConnection,15)
    stock.Score(tradingDays[-7])

def ScoreStockAll():
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,100)

    for i in range(1,3):
        stock = CScoreStock(dbConnection)
        stock.Score(tradingDays[-i])



def ToDays(tradingDays,params:list):
    result = []
    for param in params:
        start = param.get("startDay",None)
        end = param.get("endDay",None)
        if start is None:
            start = tradingDays[0]
        
        if end is None:
            end = tradingDays[-1]
        
        for tradingDay in tradingDays:
            if tradingDay >= start and tradingDay <= end:
                result.append(tradingDay)


    return result


def ScoreStock():
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,100)
    indexParam = [
        # {
        #     "startDay":"2024-01-02",
        #     "endDay":"2024-02-05",
        # },
        {
            "startDay":"2024-04-09",
            #"endDay":"2024-04-10",
            
        }
    ]

    days = ToDays(tradingDays,indexParam)
    stock = CScoreStock(dbConnection)
    stock.SelectTop80ByDates(days)
    
if __name__ == "__main__":
    Score1()
    #Score2()
    Select2()
    ScoreStockAll()
    ScoreStock()


    
