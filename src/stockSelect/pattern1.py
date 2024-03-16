import datetime
import pytz
import pandas as pd
from Utility.convertDataFrameToJPG import DataFrameToJPG


class CStockPattern1(object):
    '''
    选出最近lastNDays 日内出现过连续3个交易日涨幅大于28% 的个股
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

    def GetStockData(self,startDay,days = 3, threshold = 0.28):
        sql1 = f'''
        SELECT B.`日期`,A.`股票代码`,A.`股票简称`,B.`收盘价` FROM stock.stockbasicinfo AS A,(SELECT `日期`,`股票代码`,`收盘价` FROM stock.stockdailyinfo_2023 where `日期` >= "{startDay}" UNION SELECT `日期`,`股票代码`,`收盘价` FROM stock.stockdailyinfo where `日期` >= "{startDay}") AS B where A.`股票代码` = B.`股票代码`
        '''
        result1,columns1 = self.dbConnection.Query(sql1)
        newDf1=pd.DataFrame(result1,columns=columns1)
        groups = newDf1.groupby(["股票代码","股票简称"])
        res = []
        for stockID, group in groups:
            df = group.reset_index()
            df.dropna()
            df["收盘价"] = df["收盘价"].astype("float")
            df["3天后收盘价"] = df['收盘价'].shift(-days)
            df["1天后日期"] = df['日期'].shift(-1)
            df["3天后涨幅"] = (df["3天后收盘价"] - df["收盘价"])/df["收盘价"]
            result = df[df["3天后涨幅"] > threshold]

            if not result.empty:
                data = []
                data.extend(stockID)
                t = "; ".join(list(result["1天后日期"]))
                data.append(t)
                res.append(data)

        return res
    
    def Select(self):
        tradingDays = self.GetTradingDates()
        res = self.GetStockData(tradingDays[0])
        df = pd.DataFrame(res,columns = ["股票代码","股票简称","结果"])
        df = df[df['股票简称'].str.match('[\s\S]*(ST|退)+?[\s\S]*') == False]
        df = df[df['股票代码'].str.match('[\s\S]*(BJ|^688|^30)+?[\s\S]*') == False]
        df.to_excel('/tmp/pattern1.xlsx',index=False)
        DataFrameToJPG(df,("股票代码","股票简称"),"/tmp/","pattern1")
        print(df)

