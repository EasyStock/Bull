import datetime
import pytz
import pandas as pd
from Utility.convertDataFrameToJPG import DataFrameToJPG


class CStockPattern2(object):
    '''
    选出最近lastNDays 日内出现过连续 9个交易日出现小阳线的个股
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

    def GetStockData(self,startDay,days = 9, threshold = 9):
        sql1 = f'''
        SELECT B.`日期`,A.`股票代码`,A.`股票简称`,B.`涨跌幅` FROM stock.stockbasicinfo AS A,(SELECT `日期`,`股票代码`,`涨跌幅` FROM stock.stockdailyinfo_2023 where `日期` >= "{startDay}" UNION SELECT `日期`,`股票代码`,`涨跌幅` FROM stock.stockdailyinfo where `日期` >= "{startDay}") AS B where A.`股票代码` = B.`股票代码`
        '''
        result1,columns1 = self.dbConnection.Query(sql1)
        newDf1=pd.DataFrame(result1,columns=columns1)
        groups = newDf1.groupby(["股票代码","股票简称"])
        res = []
        for stockID, group in groups:
            df = group.reset_index()
            df.dropna(inplace=True)
            df["涨跌幅"] = df["涨跌幅"].astype("float")
            df.loc[df['涨跌幅']>0, '涨幅大于0'] = 1
            df["涨幅大于0个数"] = df['涨幅大于0'].rolling(window=days).sum()

            # df["3天后收盘价"] = df['收盘价'].shift(-days)
            key = f'''{days-1}天前日期'''
            df[key] = df['日期'].shift(days-1)
            # df["3天后涨幅"] = (df["3天后收盘价"] - df["收盘价"])/df["收盘价"]
            result = df[df["涨幅大于0个数"] > threshold]
            result = result.dropna()

            if not result.empty:
                data = []
                data.extend(stockID)
                t = "; ".join(list(result[key]))
                data.append(t)
                res.append(data)

        return res
    
    def Select(self):
        tradingDays = self.GetTradingDates()
        res = self.GetStockData(tradingDays[0],days=10,threshold=9)
        df = pd.DataFrame(res,columns = ["股票代码","股票简称","结果"])
        df = df[df['股票简称'].str.match('[\s\S]*(ST|退)+?[\s\S]*') == False]
        df = df[df['股票代码'].str.match('[\s\S]*(BJ|^688)+?[\s\S]*') == False]
        df.to_excel('/tmp/九连阳.xlsx',index=False)
        DataFrameToJPG(df,("股票代码","股票简称"),"/tmp/","九连阳")
        print(df)

