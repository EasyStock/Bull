from mysql.connect2DB import ConnectToDB
from score.scoreMgr import CScoreMgr
from DBOperating import GetTradingDateLastN
from score.scoreZai import CScoreZaiMgr,CSelectZai

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
    
    indexParam = {
        "抗跌分数":{
            "startDay":"2024-01-02",
            "endDay":"2024-02-05",
        },
        "领涨分数":{
            "startDay":"2024-02-06",
            "endDay":"2024-02-28",
        }
    }
    mgr = CScoreMgr(dbConnection)
    mgr.Score(indexParam)
    mgr.Select(indexParam)
    dates = ("2024-02-06","2024-02-08","2024-02-19","2024-02-20","2024-02-21","2024-02-22","2024-02-23","2024-02-26","2024-02-27","2024-02-28")
    mgr.UnionSelect(dates)

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
            "startDay":"2024-01-02",
            "endDay":"2024-02-05",
        },
        "领涨分数":{
            "startDay":"2024-02-06",
            "endDay":"2024-02-28",
        }
    }
    tradingDays = GetTradingDateLastN(dbConnection,15)
    select = CSelectZai(dbConnection,tradingDays)
    select.Select(indexParam)


if __name__ == "__main__":
    #Score1()
    #Score2()
    Select2()


    
