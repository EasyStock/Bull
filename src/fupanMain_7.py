import os
from mysql.connect2DB import ConnectToDB
from fupan.yiziban import CYizhiban
from fupan.tradingDate import GetTradingDateLastN
from fupan.zhuanQianXiaoying import CZhuanQianXiaoXing
import pandas as pd
import logging
from workspace import workSpaceRoot,WorkSpaceFont
logger = logging.getLogger()

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
        "\n#以下是一字板SQL",
        f'''SELECT * FROM stock.yiziban where `日期` = "{tradingDays[-1]}"  order by `连续涨停天数` DESC;''',
        f'''SELECT * FROM stock.yiziban where `日期` = "{tradingDays[-2]}"  order by `连续涨停天数` DESC;''',
        f'''SELECT * FROM stock.yiziban where `日期` >= "{tradingDays[-3]}" order by `日期` ASC;''',
        
        "\n#以下是市场情绪",
        f'''SELECT * FROM stock.fuPan where `日期` > "{tradingDays[-35]}" order by `日期`;''',
        f'''SELECT * FROM stock.`市场总体情绪` where `日期` > "{tradingDays[-15]}" order by `日期`;''',
        f'''SELECT * FROM stock.`超短环境1`  where `日期` > "{tradingDays[-15]}" order by `日期`;''',
        f'''SELECT * FROM stock.`超短环境2`  where `日期` > "{tradingDays[-15]}" order by `日期`;''',
        f'''SELECT * FROM stock.`超短情绪指标`  where `日期` > "{tradingDays[-15]}" order by `日期`;''',
        f'''SELECT * FROM stock.`超短情绪指标2`  where `日期` > "{tradingDays[-15]}" order by `日期`;''',
       
        "\n#以下是今日操作SQL",
        f'''SELECT * FROM stock.caozuo where `日期` >= "{tradingDays[-3]}";''',

        "\n#以下是龙虎榜",
        f'''select * from `stock`.`dragon`  where date = "{tradingDays[-1]}";''',
        f'''#东北证券股份有限公司绍兴金柯桥大道证券营业部,中国银河证券股份有限公司大连金马路证券营业部,财信证券股份有限公司杭州西湖国贸中心证券营业部,天风证券股份有限公司上海浦东分公司''',
        f'''select * from `stock`.`dragon` where operator_ID in (10638005,10657404,10656871,10944812) and date = "{tradingDays[-1]}"; ''', #东北证券股份有限公司绍兴金柯桥大道证券营业部,中国银河证券股份有限公司大连金马路证券营业部

        "\n#以下是今日赚钱效应SQL",
        f'''SELECT A.*,B.`股票简称`, B.`连续涨停天数` As `昨日连续涨停天数`,B.`涨停原因类别` As `昨日涨停原因类别` FROM stock.stockdailyinfo As A, (SELECT `股票代码`,`股票简称`,`连续涨停天数`,`涨停原因类别` FROM stock.stockzhangting where `日期` = "{tradingDays[-2]}") As B where A.`日期` = "{tradingDays[-1]}" and A.`股票代码` = B.`股票代码` order by A.`股票代码`DESC;''',

        "\n#以下是今日涨停连板梯队",
        f'''SELECT A.`日期`,A.`股票代码`,B.`股票简称`, A.`成交量`, A.`成交额`, B.`连续涨停天数` As `涨停天数`,B.`首次涨停时间`,B.`涨停原因类别` As `今日涨停原因类别` FROM stock.stockdailyinfo As A, (SELECT `股票代码`,`股票简称`,`连续涨停天数`,`涨停原因类别`,`首次涨停时间` FROM stock.stockzhangting where `日期` = "{tradingDays[-1]}") As B where A.`日期` = "{tradingDays[-1]}" and A.`股票代码` = B.`股票代码` order by B.`连续涨停天数`DESC,B.`首次涨停时间`ASC ;''',

        "\n#以下是新增概念查询",
        f'''SELECT * FROM stock.gainian where `概念名称` not in (SELECT `概念名称` FROM stock.gainian where `更新日期`="{tradingDays[-2]}") and `更新日期`="{tradingDays[-1]}";''',
        f'''SELECT * FROM stock.gainian where `概念名称` not in (SELECT `概念名称` FROM stock.gainian where `更新日期`="{tradingDays[-1]}") and `更新日期`="{tradingDays[-2]}";''',
        f'''SELECT * FROM stock.stockbasicinfo where `所属概念` like "%超临界发电%";''',
        f'''SELECT A.`日期`,A.`转债代码`,A.`转债名称`,A.`现价`,A.`正股名称`,B.`所属概念` FROM `stock`.`kezhuanzhai` as A,`stock`.`stockBasicInfo` AS B where A.`正股名称`=B.`股票简称` and `日期`='{tradingDays[-1]}' and (B.`所属概念` like '%比亚迪%'  OR B.`所属概念` like '%比亚迪概念%' ) order by `PB` DESC;''',

        f"\n#以下是{tradingDays[-45]} 之后周期内的高标",
        f'''select A.`股票代码`,B.`股票简称`,A.`最大连板天数` from (SELECT `股票代码`,max(`连续涨停天数`) As `最大连板天数` FROM stock.stockzhangting where `日期`> "{tradingDays[-45]}" and `连续涨停天数`>=4 group by `股票代码` order by `最大连板天数` DESC) As A, `stockBasicInfo` as B where A.`股票代码`=B.`股票代码`''',
        
        f"\n#以下是 反包",
        f'''select * from  stock.stockzhangting where `日期` in ("{tradingDays[-3]}","{tradingDays[-4]}","{tradingDays[-5]}","{tradingDays[-6]}") and `股票代码` in (select `股票代码` from stock.stockzhangting where `日期` = "{tradingDays[-1]}") and `股票代码` not in (select `股票代码` from stock.stockzhangting where `日期` = "{tradingDays[-2]}") order by `日期` DESC;''',
        f'''select * from  stock.stockzhangting where `股票代码` in (select `股票代码` from  stock.stockzhangting where `日期` in ("{tradingDays[-3]}","{tradingDays[-4]}","{tradingDays[-5]}","{tradingDays[-6]}") and `股票代码` in (select `股票代码` from stock.stockzhangting where `日期` = "{tradingDays[-1]}") and `股票代码` not in (select `股票代码` from stock.stockzhangting where `日期` = "{tradingDays[-2]}") ) and `日期` = "{tradingDays[-1]}";''',

        f'''\n#=========================================以上是复盘SQL=======================================================\n\n\n\n''',
    ]
    fullName = f'''{workSpaceRoot}/复盘/股票/{tradingDays[-1]}/复盘SQL_{tradingDays[-1]}.sql'''
    with open(fullName,'w+') as f:
        for sql in sqls:
            f.write(sql+'\n')

def ConvertDataFrameToJPG(df,fullPath):
    from pandas.plotting import table
    import matplotlib.pyplot as plt
    plt.rcParams["font.sans-serif"] = [WorkSpaceFont]#显示中文字体
    high = int(0.174 * df.shape[0]+0.5)+1
    fig = plt.figure(figsize=(3, high), dpi=200)#dpi表示清晰度
    ax = fig.add_subplot(111, frame_on=False) 
    ax.xaxis.set_visible(False)  # hide the x axis
    ax.yaxis.set_visible(False)  # hide the y axis
    table(ax, df, loc='center')  # 将df换成需要保存的dataframe即可
    plt.savefig(fullPath)


def GetZhouqiGaoBiao(dbConnection,tradingDay,today):
    sql = f'''select A.`股票代码`,B.`股票简称`,A.`最大连板天数` from (SELECT `股票代码`,max(`连续涨停天数`) As `最大连板天数` FROM stock.stockzhangting where `日期`> "{tradingDay}" and `连续涨停天数`>=4 group by `股票代码` order by `最大连板天数` DESC) As A, `stockBasicInfo` as B where A.`股票代码`=B.`股票代码`'''
    data, columns = dbConnection.Query(sql)
    df = pd.DataFrame(data,columns=columns)
    fodler = f'{workSpaceRoot}/复盘/股票/{today}/'
    if os.path.exists(fodler) == False:
        os.makedirs(fodler)
    fullPath = f'''{fodler}周期高标_{today}.xlsx'''
    df.to_excel(fullPath,index=False)

    rootDir = f"{workSpaceRoot}/复盘/股票/{today}/"
    if os.path.exists(rootDir) == False:
        os.makedirs(rootDir)

    fullPath = f"{rootDir}周期高标_{today}.jpg"
    jpgDataFrame = pd.DataFrame(df,columns=["股票代码","股票简称"])
    ConvertDataFrameToJPG(jpgDataFrame,fullPath)
    logger.info(fullPath)


def GetFuPanList(dbConnection,tradingDay):
    sql = f'''SELECT * FROM stock.stockzhangting where 日期 = "{tradingDay}" order by `连续涨停天数` DESC, `首次涨停时间` ASC ,`最终涨停时间` ASC;'''
    data, columns = dbConnection.Query(sql)
    df = pd.DataFrame(data,columns=columns)
    df["明日预期"] = ""

    rootDir = f"{workSpaceRoot}/复盘/股票/{tradingDay}/"
    if os.path.exists(rootDir) == False:
        os.makedirs(rootDir)

    fullPath = f'''{rootDir}明日预期_{tradingDay}.xlsx'''
    df.to_excel(fullPath,index=False)

    fullPath = f"{rootDir}高标_{tradingDay}.jpg"
    jpgDataFrame = pd.DataFrame(df,columns=["股票代码","股票简称"])
    ConvertDataFrameToJPG(jpgDataFrame,fullPath)
    logger.info(fullPath)

    lianbanDf = df[df["连续涨停天数"]>=2]
    fullPath = f"{rootDir}连板_{tradingDay}.jpg"
    jpgDataFrame = pd.DataFrame(lianbanDf,columns=["股票代码","股票简称"])
    ConvertDataFrameToJPG(jpgDataFrame,fullPath)
    logger.info(fullPath)

    shoubanDF =  df[df["连续涨停天数"]==1]
    fullPath = f"{rootDir}首板_{tradingDay}.jpg"
    jpgDataFrame = pd.DataFrame(shoubanDF,columns=["股票代码","股票简称"])
    ConvertDataFrameToJPG(jpgDataFrame,fullPath)
    logger.info(fullPath)
    

def FupanDaily():
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,70)
    # #print(tradingDays)
    
    yiziban = CYizhiban(dbConnection,tradingDays[-1])
    yiziban.YiZhiBan()
    zhuanq = CZhuanQianXiaoXing(dbConnection,tradingDays[-2],tradingDays[-1])
    zhuanq.ZhuanQianXiaoYing()
    PrintSQLs(tradingDays)
    GetFuPanList(dbConnection,tradingDays[-1])
    GetZhouqiGaoBiao(dbConnection,tradingDays[-45],tradingDays[-1])

    # for i in range(2,len(tradingDays)):
    #     print(tradingDays[i])
    #     zhuanq = CZhuanQianXiaoXing(dbConnection,tradingDays[i-1],tradingDays[i])
    #     zhuanq.ZhuanQianXiaoYing()
    #     #input()
    # 
    #   
if __name__ == "__main__":
    FupanDaily()
