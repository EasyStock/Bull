import pandas as pd
import json
from message.feishu.messageformat_feishu import FormatCardOfNewGaiNian,FormatCardOfMeiRiFuPan
from message.feishu.webhook_api import sendMessageByWebhook
from Utility.convertDataFrameToJPG import DataFrameToJPG
import os
from workspace import workSpaceRoot,GetStockFolder

class CReDian(object):
    def __init__(self,dbConnection,today):
        self.dbConnection = dbConnection
        self.reDianBankuai1 = None
        self.reDianBankuai2 = None
        self.reDianBankuai1DF = None
        self.reDianBankuai2DF = None
        self.today = today
    
    def _GetZhangTingData(self):
        sql = f'''select A.*,B.`成交额`,C.`流通市值`,C.`所属概念` from  stock.stockzhangting AS A,stock.stockdailyinfo As B, stock.stockbasicinfo AS C where A.`日期`  = "{self.today}"  and B.`日期`  = "{self.today}"  and A.`股票代码` = B.`股票代码` and A.`股票代码` = C.`股票代码` order by A.`连续涨停天数` DESC,A.`涨停关键词` DESC,A.`最终涨停时间` ASC;'''
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)
        df["连续涨停天数"] = df["连续涨停天数"].astype(int)
        return df
    
    def AnalysisZhangTingReason(self):
        df = self._GetZhangTingData()
        reasons = []
        for _, row in df.iterrows():
            reason = row["涨停原因类别"]
            reasons.extend(reason.split("+"))

        reasons = list(set(reasons))
        reasonResults = {}
        for reason in reasons:
            sql = f"select count(*) from `stockZhangting`AS A, `stockBasicInfo` As B where A.`股票代码`= B.`股票代码` and `日期` = '{self.today}' and A.`涨停原因类别` like '%{reason}%' ;"
            result ,_ = self.dbConnection.Query(sql)
            count = result[0][0]
            reasonResults[reason] = count

        ret = sorted(reasonResults.items(), key=lambda d: d[1],reverse=True)
        self.reDianBankuai1 = ret[0][0]
        self.reDianBankuai2 = ret[1][0]
        self.reDianBankuai1DF = df[df["涨停原因类别"].str.contains(self.reDianBankuai1)]
        self.reDianBankuai2DF = df[df["涨停原因类别"].str.contains(self.reDianBankuai2)]
        self.reDianBankuai1DF.reset_index(inplace=True)
        self.reDianBankuai2DF.reset_index(inplace=True)

        self.lianbanDF = df[df["连续涨停天数"]>=2]
        self.shoubanDF = df[df["连续涨停天数"]==1]
        self.lianbanDF.reset_index(inplace=True)
        self.shoubanDF.reset_index(inplace=True)

        sql = f'''REPLACE INTO `stock`.`rediandaily` (`日期`, `热点`) VALUES ('{self.today}', '{self.reDianBankuai1};{self.reDianBankuai2}');'''
        self.dbConnection.Execute(sql)
        # # print(self.reDianBankuai1DF)
        # # print(self.reDianBankuai2DF)
        # for r in ret[:2]:
        #     if r[1] >=2:
        #         print(r)

def SendNewGaiNianOfStock(dbConnection,tradingDays,webhook,secret):
    #股市新增概念
    sql = f'''SELECT * FROM stock.gainian where `概念名称` not in (SELECT `概念名称` FROM stock.gainian where `更新日期`="{tradingDays[-2]}") and `更新日期`="{tradingDays[-1]}";'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    if df.empty:
        return

    if len(results) >=5:
        results = results[:5]
        
    for result in results:
        gainian = result[0]
        if len(gainian) == 0:
            continue
        sql1 = f'''INSERT IGNORE INTO `stock`.`stockgainiannew` (`日期`, `新概念`) VALUES ('{tradingDays[-1]}', '{gainian}');'''
        dbConnection.Execute(sql1)

        sql2 = f'''SELECT `股票代码`, `股票简称` FROM stock.stockbasicinfo where `所属概念` like "%{gainian}%";'''
        results1, columns = dbConnection.Query(sql2)

        jpgDataFrame = pd.DataFrame(results1,columns=columns)
        if not jpgDataFrame.empty:
            folderRoot= GetStockFolder(tradingDays[-1])
            fileName = f'''新增概念_{gainian}'''.replace("/", "")
            DataFrameToJPG(jpgDataFrame,columns,folderRoot,fileName)

        msg = FormatCardOfNewGaiNian(tradingDays[-1],gainian,results1,["**股票代码**","**股票简称**"])
        content = json.dumps(msg,ensure_ascii=False)
        msg_type = "interactive"
        sendMessageByWebhook(webhook,secret,msg_type,content)

        s = ";".join([f'''{t[0]} {t[1]}''' for t in results1])
        sql3 = f'''UPDATE `stock`.`stockgainiannew` SET `股票` = '{s}' WHERE (`日期` = '{tradingDays[-1]}') and (`新概念` = '{gainian}');'''
        dbConnection.Execute(sql3)


def SendMeiRiFuPan_Stock(dbConnection,tradingDays,webhook,secret):
    #每日复盘
    last3Days = tuple(tradingDays[-3:])
    sql = f'''SELECT * FROM stock.fuPan where `日期` in {last3Days} ;'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    if df.empty:
        return
    sql2 = f'''SELECT max(`连板数量`) as `最高连板数量`,min(`连板数量`) as `最低连板数量`,avg(`连板数量`) as `平均连板数量`,max(`高度板`) as `最大高度板`,max(`2连板个数`) as `最大2连板个数` FROM (SELECT * FROM stock.fuPan order by `日期` DESC limit 30) as A;'''
    results2, _ = dbConnection.Query(sql2)
    df['最高连板数量'] = results2[0][0]
    df['最低连板数量'] = results2[0][1]
    df['平均连板数量'] = results2[0][2]
    df['最大高度板'] = results2[0][3]
    df['最大2连板个数'] = results2[0][4]
    redian = CReDian(dbConnection,tradingDays[-1])
    redian.AnalysisZhangTingReason()
    redianStr = f'''{redian.reDianBankuai1} 和 {redian.reDianBankuai2}'''
    redianDfs = (redian.reDianBankuai1DF,redian.reDianBankuai2DF)
    msg = FormatCardOfMeiRiFuPan(tradingDays[-1],df,redianStr,redianDfs)
    content = json.dumps(msg,ensure_ascii=False)
    msg_type = "interactive"
    sendMessageByWebhook(webhook,secret,msg_type,content)

