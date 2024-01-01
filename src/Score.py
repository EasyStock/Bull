from mysql.connect2DB import ConnectToDB
from score.scoreMgr import CScoreMgr

from message.feishu.webhook_zhuanzai import SendkeZhuanZaiScore

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
    indexParam = {
        "抗跌分数":{
            "startDay":"2023-12-05",
            "endDay":"2023-12-20",
        },
        "领涨分数":{
            "startDay":"2023-12-27",
            "endDay":"2023-12-29",
        }
    }
    maxDate = max(indexParam ["抗跌分数"]["endDay"],indexParam ["领涨分数"]["endDay"])
    mgr = CScoreMgr(dbConnection)
    sql = f"SELECT `日期` FROM stock.treadingDay where `开市` =1 and `交易所`='SSE' and `日期`>='{maxDate}'"
    res,_ = dbConnection.Query(sql)
    results = [r[0] for r in res]
    date = results[0]
    diDianDate = indexParam ["抗跌分数"]["endDay"] #最低点的时候价格
    #mgr.Score(date,indexParam)
    #mgr.Select(date,diDianDate)
    webhook = "https://open.feishu.cn/open-apis/bot/v2/hook/e156ab0d-9d9d-4bc4-a4b5-faf9ad6344c2"
    secret = "chzCzY4VkzctfN2qvtxARg"
    
    SendkeZhuanZaiScore(dbConnection,date,diDianDate,webhook,secret,4.8)