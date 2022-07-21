import os
from mysql.connect2DB import ConnectToDB
from fupan.yiziban import CYizhiban
from fupan.tradingDate import GetTradingDateLastN
from fupan.zhuanQianXiaoying import CZhuanQianXiaoXing
import pandas as pd

def formatSql_1(operator1, operator2, net,descption):
    # 两个营业部同时净买入卖出
    sqls = [
        f'''\n#=========={operator1} 与 {operator2} {descption} 共同买入 与卖出净额超{net/10000} 万元 =============''',
        f'''select * from `stock`.`dragon` where (operator_ID = {operator1} or operator_ID = {operator2})  and `flag` = "S" and (date,stockID) in (select date,stockID from `stock`.`dragon` where operator_ID = {operator2} and `flag` = "S" and (NET < -{net}) and (date,stockID) in (select date,stockID from `stock`.`dragon` where operator_ID = {operator1} and `flag` = "S" and (NET < -{net})));
''',
        f'''select * from `stock`.`dragon` where (operator_ID = {operator1} or operator_ID = {operator2})  and `flag` = "B" and (date,stockID) in (select date,stockID from `stock`.`dragon` where operator_ID = {operator2} and `flag` = "B" and (NET > {net}) and (date,stockID) in (select date,stockID from `stock`.`dragon` where operator_ID = {operator1} and `flag` = "B" and (NET > {net})));
''',
    ]
    return sqls


def PrintSQLs(tradingDays):
    sqls = [
        "\n#========================以下是复盘SQL==============================",
        f'''SELECT * FROM stock.fuPan where `日期` > "{tradingDays[-15]}" order by `日期`;''',
        f'''SELECT * FROM stock.`市场总体情绪` where `日期` > "{tradingDays[-15]}" order by `日期`;''',
        f'''SELECT * FROM stock.`超短环境1`  where `日期` > "{tradingDays[-15]}" order by `日期`;''',
        f'''SELECT * FROM stock.`超短环境2`  where `日期` > "{tradingDays[-15]}" order by `日期`;''',
        f'''SELECT * FROM stock.`超短情绪指标`  where `日期` > "{tradingDays[-15]}" order by `日期`;''',
       
        "\n#========================以下是今日操作SQL==============================",
        f'''SELECT * FROM stock.caozuo where `日期` >= "{tradingDays[-3]}";''',

        "\n#========================以下是一字板SQL==============================",
        f'''SELECT * FROM stock.yiziban where `日期` = "{tradingDays[-1]}"  order by `连续涨停天数` DESC;''',
        f'''SELECT * FROM stock.yiziban where `日期` = "{tradingDays[-2]}"  order by `连续涨停天数` DESC;''',
        f'''SELECT * FROM stock.yiziban where `日期` >= "{tradingDays[-3]}" order by `日期` ASC;''',
        
        "\n#========================以下是龙虎榜==============================",
        f'''select * from `stock`.`dragon`  where date = "{tradingDays[-1]}";''',
        f'''#东北证券股份有限公司绍兴金柯桥大道证券营业部,中国银河证券股份有限公司大连金马路证券营业部,财信证券股份有限公司杭州西湖国贸中心证券营业部,天风证券股份有限公司上海浦东分公司''',
        f'''select * from `stock`.`dragon` where operator_ID in (10638005,10657404,10656871,10944812) and date = "{tradingDays[-1]}"; ''', #东北证券股份有限公司绍兴金柯桥大道证券营业部,中国银河证券股份有限公司大连金马路证券营业部

        "\n#========================以下是今日赚钱效应SQL==============================",
        f'''SELECT A.*,B.`股票简称`, B.`连续涨停天数` As `昨日连续涨停天数`,B.`涨停原因类别` As `昨日涨停原因类别` FROM stock.stockdailyinfo As A, (SELECT `股票代码`,`股票简称`,`连续涨停天数`,`涨停原因类别` FROM stock.stockzhangting where `日期` = "{tradingDays[-2]}") As B where A.`日期` = "{tradingDays[-1]}" and A.`股票代码` = B.`股票代码` order by A.`股票代码`DESC;''',

        "\n#========================以下是新增概念查询==============================",
        f'''SELECT * FROM stock.gainian where `概念名称` not in (SELECT `概念名称` FROM stock.gainian where `更新日期`="{tradingDays[-2]}") and `更新日期`="{tradingDays[-1]}";''',
        f'''SELECT * FROM stock.gainian where `概念名称` not in (SELECT `概念名称` FROM stock.gainian where `更新日期`="{tradingDays[-1]}") and `更新日期`="{tradingDays[-2]}";''',
        f'''SELECT * FROM stock.stockbasicinfo where `所属概念` like "%超临界发电%";''',
        f'''SELECT A.`日期`,A.`转债代码`,A.`转债名称`,A.`现价`,A.`正股名称`,B.`所属概念` FROM `stock`.`kezhuanzhai` as A,`stock`.`stockBasicInfo` AS B where A.`正股名称`=B.`股票简称` and `日期`='2022-06-10' and (B.`所属概念` like '%比亚迪%'  OR B.`所属概念` like '%比亚迪概念%' ) order by `PB` DESC;''',

    ]

    for sql in sqls:
        print(sql)
    

def GetFuPanList(dbConnection,tradingDay):
    sql = f'''SELECT * FROM stock.stockzhangting where 日期 = "{tradingDay}" order by `连续涨停天数` DESC, `首次涨停时间` ASC;'''
    data, columns = dbConnection.Query(sql)
    df = pd.DataFrame(data,columns=columns)
    df["明日预期"] = ""
    fodler = f'/Volumes/Data/复盘/股票/没日涨停复盘/'
    if os.path.exists(fodler) == False:
        os.makedirs(fodler)
    fullPath = f'''{fodler}{tradingDay}.xlsx'''
    df.to_excel(fullPath,index=False)

if __name__ == "__main__":
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,70)
    #print(tradingDays)
    
    yiziban = CYizhiban(dbConnection,tradingDays[-1])
    yiziban.YiZhiBan()
    zhuanq = CZhuanQianXiaoXing(dbConnection,tradingDays[-2],tradingDays[-1])
    zhuanq.ZhuanQianXiaoYing()
    PrintSQLs(tradingDays)
    GetFuPanList(dbConnection,tradingDays[-1])

    # for i in range(2,len(tradingDays)):
    #     print(tradingDays[i])
    #     zhuanq = CZhuanQianXiaoXing(dbConnection,tradingDays[i-1],tradingDays[i])
    #     zhuanq.ZhuanQianXiaoYing()
    #     #input()