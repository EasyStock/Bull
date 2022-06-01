from mysql.connect2DB import ConnectToDB
from fupan.yiziban import CYizhiban
from fupan.tradingDate import GetTradingDateLastN
from fupan.zhuanQianXiaoying import CZhuanQianXiaoXing


def PrintSQLs(tradingDays):
    sqls = [
        "\n#========================以下是复盘SQL==============================",
        f'''SELECT * FROM stock.fuPan where `日期` > "{tradingDays[-15]}" order by `日期`;''',
        f'''SELECT * FROM stock.`市场总体情绪` where `日期` > "{tradingDays[-15]}" order by `日期`;''',
        f'''SELECT * FROM stock.`超短环境`  where `日期` > "{tradingDays[-15]}" order by `日期`;''',
        f'''SELECT * FROM stock.`超短情绪指标`  where `日期` > "{tradingDays[-15]}" order by `日期`;''',
       
        "\n#========================以下是今日操作SQL==============================",
        f'''SELECT * FROM stock.caozuo where `日期` >= "{tradingDays[-3]}";''',

        "\n#========================以下是一字板SQL==============================",
        f'''SELECT * FROM stock.yiziban where `日期` = "{tradingDays[-1]}";''',
        f'''SELECT * FROM stock.yiziban where `日期` = "{tradingDays[-2]}";''',
        f'''SELECT * FROM stock.yiziban where `日期` >= "{tradingDays[-3]}";''',
        
        "\n#========================以下是龙虎榜==============================",
        f'''select * from `stock`.`dragon`  where date = "{tradingDays[-1]}";''',
        
        "\n#========================以下是今日赚钱效应SQL==============================",
        f'''SELECT A.*,B.`股票简称`, B.`连续涨停天数` As `昨日连续涨停天数`,B.`涨停原因类别` As `昨日涨停原因类别` FROM stock.stockdailyinfo As A, (SELECT `股票代码`,`股票简称`,`连续涨停天数`,`涨停原因类别` FROM stock.stockzhangting where `日期` = "{tradingDays[-2]}") As B where A.`日期` = "{tradingDays[-1]}" and A.`股票代码` = B.`股票代码` order by A.`股票代码`DESC;''',

    ]

    for sql in sqls:
        print(sql)


if __name__ == "__main__":
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,70)
    #print(tradingDays)
    
    yiziban = CYizhiban(dbConnection,tradingDays[-1])
    yiziban.YiZhiBan()
    zhuanq = CZhuanQianXiaoXing(dbConnection,tradingDays[-2],tradingDays[-1])
    zhuanq.ZhuanQianXiaoYing()
    PrintSQLs(tradingDays)

    # for i in range(2,len(tradingDays)):
    #     print(tradingDays[i])
    #     zhuanq = CZhuanQianXiaoXing(dbConnection,tradingDays[i-1],tradingDays[i])
    #     zhuanq.ZhuanQianXiaoYing()
    #     #input()