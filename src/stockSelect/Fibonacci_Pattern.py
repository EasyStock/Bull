import datetime
import pytz
import pandas as pd
from Utility.convertDataFrameToJPG import DataFrameToJPG
from workspace import GetStockFolder

class CFibonacciPattern(object):
    '''
    N型选股器: 斐波那契选股
    
    选股要求:
    0. 去最近涨停N天的最低，最高点，最高点在最低点右侧
    1. 今天的收盘价占整个上涨序列的百分比，取 0.618 和0.5 附近

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

    def GetStockData(self,startDay,days = 30):
        sql1 = f'''
        SELECT B.`日期`,A.`股票代码`,A.`股票简称`,B.`收盘价`,B.`开盘价`,B.`涨跌幅` FROM stock.stockbasicinfo AS A,(SELECT `日期`,`股票代码`,`开盘价`,`收盘价`,`涨跌幅` FROM stock.stockdailyinfo_2024 where `日期` >= "{startDay}" UNION SELECT `日期`,`股票代码`,`开盘价`,`收盘价`,`涨跌幅` FROM stock.stockdailyinfo where `日期` >= "{startDay}") AS B where A.`股票代码` = B.`股票代码`
        '''
        result1,columns1 = self.dbConnection.Query(sql1)
        newDf1=pd.DataFrame(result1,columns=columns1)
        newDf1["开盘价"] = newDf1["开盘价"].astype("float").round(3)
        newDf1["收盘价"] = newDf1["收盘价"].astype("float").round(3)

        groups = newDf1.groupby(["股票代码","股票简称"])
        res = []
        for stockID, group in groups:
            df = group.reset_index()
            df.dropna()
            # if stockID[0] == "002272.SZ":
            #     print(df)
            if df.shape[0] < days:
                continue

            last_max_index = df['收盘价'][::-1].idxmax()
            last_max_row = df.loc[last_max_index]

            last_min_index = df['收盘价'][::-1].idxmin()
            last_min_row = df.loc[last_min_index]
     
            lastLine = df.iloc[-1]
            if last_max_row["日期"] < last_min_row["日期"]: # 下降通道
                continue

            zhangfu = (last_max_row["收盘价"] - last_min_row["收盘价"])/last_min_row["收盘价"]
            ratio = (lastLine["收盘价"] - last_min_row["收盘价"])/(last_max_row["收盘价"] - last_min_row["收盘价"])
            # if zhangfu < 0.2:
            #     continue

            # if ratio <0.5:
            #     continue

            data = []
            data.extend(stockID)
            t = f'''{last_min_row["日期"]};{last_max_row["日期"]};{zhangfu};{ratio}'''
            data.append(t)
            res.append(data)

        return res
    
    def Select(self):
        tradingDays = self.GetTradingDates()
        res = self.GetStockData(tradingDays[-30])
        df = pd.DataFrame(res,columns = ["股票代码","股票简称","结果"])
        df = df[df['股票简称'].str.match('[\s\S]*(ST|退)+?[\s\S]*') == False]
        df = df[df['股票代码'].str.match('[\s\S]*(BJ|^688|^30)+?[\s\S]*') == False]
        df.sort_values("结果",ascending=False,inplace=True)
        root = GetStockFolder(tradingDays[-1])
        df.to_excel('/tmp/斐波那契选股器.xlsx',index=False)
        #DataFrameToJPG(df,("股票代码","股票简称"),root,"斐波那契选股器")
        print(df)



