import datetime
import pytz
import pandas as pd
from Utility.convertDataFrameToJPG import DataFrameToJPG
from workspace import GetStockFolder

class CStockPattern4(object):
    '''
    最近几个交易日出现过新低的股票
    '''
    def __init__(self,dbConnection,lastNDays = 200):
        self.dbConnection = dbConnection
        self.lastNDays = lastNDays
    
    def GetTradingDates(self):
        today = datetime.datetime.now(pytz.timezone('Asia/Shanghai')).date()
        end = today.strftime("%Y-%m-%d")
        sql = f"SELECT `日期` FROM stock.treadingDay where `开市` =1 and `交易所`='SSE' and `日期`<='{end}' order by `日期` DESC limit {self.lastNDays};"
        res,_ = self.dbConnection.Query(sql)
        self.tradingDays = [r[0] for r in reversed(res)]
        return self.tradingDays

    def GetStockData(self,startDay,threshold):
        sql1 = f'''
        SELECT B.`日期`,A.`股票代码`,A.`股票简称`,B.`收盘价` ,B.`最低价` FROM stock.stockbasicinfo AS A,(SELECT `日期`,`股票代码`,`收盘价`,`最低价` FROM stock.stockdailyinfo_2023 where `日期` >= "{startDay}" UNION SELECT `日期`,`股票代码`,`收盘价` ,`最低价` FROM stock.stockdailyinfo where `日期` >= "{startDay}") AS B where A.`股票代码` = B.`股票代码`
        '''
        result1,columns1 = self.dbConnection.Query(sql1)
        newDf1=pd.DataFrame(result1,columns=columns1)
        groups = newDf1.groupby(["股票代码","股票简称"])
        res = []
        for stockID, group in groups:
            df = group.reset_index()
            if df.shape[0] <= 20:
                continue
            df.dropna(inplace=True)
            df["收盘价"] = df["收盘价"].astype("float")
            df["最低价"] = df["最低价"].astype("float")
            min_value_row = df.loc[df['收盘价'].idxmin()]
            min_value_row1 = df.loc[df['最低价'].idxmin()]
            if min_value_row1["日期"] >= threshold:
                data = []
                data.extend(stockID)
                data.append(min_value_row["日期"])
                data.append("最低价")
                res.append(data)
                continue

            if min_value_row["日期"] >= threshold:
                data = []
                data.extend(stockID)
                data.append(min_value_row["日期"])
                data.append("收盘价")
                res.append(data)

        return res
    
    def Select(self):
        tradingDays = self.GetTradingDates()
        tradingDays = tradingDays[-90:]
        threshold = tradingDays[-7]
        res = self.GetStockData(tradingDays[0],threshold)
        df = pd.DataFrame(res,columns = ["股票代码","股票简称","结果","结果1"])
        df = df[df['股票简称'].str.match('[\s\S]*(ST|退|C)+?[\s\S]*') == False]
        df = df[df['股票代码'].str.match('[\s\S]*(BJ|^688)+?[\s\S]*') == False]
        root = GetStockFolder(tradingDays[-1])
        df.to_excel(f'''{root}/近7日内创新低.xlsx''',index=False)
        DataFrameToJPG(df,("股票代码","股票简称"),root,"近7日内创新低")
        print(df)

