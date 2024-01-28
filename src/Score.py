from mysql.connect2DB import ConnectToDB
from score.scoreMgr import CScoreMgr



if __name__ == "__main__":
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

    indexParam = {
        "抗跌分数":{
            "startDay":"2024-01-02",
            "endDay":"2024-01-23",
        },
        "领涨分数":{
            "startDay":"2024-01-24",
            "endDay":"2024-01-25",
        }
    }
    
    mgr = CScoreMgr(dbConnection)
    mgr.Score(indexParam)
    mgr.Select(indexParam)


    
