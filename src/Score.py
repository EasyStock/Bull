from score.score import scoreV
from mysql.connect2DB import ConnectToDB
from score.scoreMgr import CScoreMgr



if __name__ == "__main__":
    dbConnection = ConnectToDB()
    indexParam = {
        "抗跌分数":{
            "startDay":"2023-11-21",
            "endDay":"2023-12-06",
        },
        "活跃度分数":{
            "startDay":"2023-10-24",
            "endDay":"2023-11-15",
        }
    }
    mgr = CScoreMgr(dbConnection)
    date = "2023-12-15"
    mgr.Score(date,indexParam)
    mgr.Select(date)