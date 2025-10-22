import datetime
import pytz
import pandas as pd
from Utility.convertDataFrameToJPG import DataFrameToJPG
import numpy as np
from workspace import GetStockFolder

class CPattern14(object):
    '''
    平地战法: 前N 个交易日波动率很低
    '''
    def __init__(self,dbConnection,lastNDays = 500):
        self.dbConnection = dbConnection
        self.lastNDays = lastNDays
    
    def GetTradingDates(self):
        today = datetime.datetime.now(pytz.timezone('Asia/Shanghai')).date()
        end = today.strftime("%Y-%m-%d")
        sql = f"SELECT `日期` FROM stock.treadingday where `开市` =1 and `交易所`='SSE' and `日期`<='{end}' order by `日期` DESC limit {self.lastNDays};"
        res,_ = self.dbConnection.Query(sql)
        self.tradingDays = [r[0] for r in reversed(res)]
        return self.tradingDays

    def GetStockData(self,startDay,window_size = 30):
        sql1 = f'''
        SELECT B.`日期`,A.`股票代码`,A.`股票简称`,B.`收盘价`,B.`开盘价`,B.`涨跌幅`,B.`成交量` FROM stock.stockbasicinfo AS A,(SELECT `日期`,`股票代码`,`开盘价`,`收盘价`,`涨跌幅`,`成交量` FROM stock.stockdailyinfo_2024 where `日期` >= "{startDay}" UNION SELECT `日期`,`股票代码`,`开盘价`,`收盘价`,`涨跌幅`,`成交量` FROM stock.stockdailyinfo where `日期` >= "{startDay}") AS B where A.`股票代码` = B.`股票代码`
        '''
        result1,columns1 = self.dbConnection.Query(sql1)
        newDf1=pd.DataFrame(result1,columns=columns1)
        newDf1["开盘价"] = newDf1["开盘价"].astype("float").round(3)
        newDf1["收盘价"] = newDf1["收盘价"].astype("float").round(3)
        newDf1["涨跌幅"] = newDf1["涨跌幅"].astype("float").round(3)
        newDf1["成交量"] = newDf1["成交量"].astype("float").round(3)

        groups = newDf1.groupby(["股票代码","股票简称"])
        res = []
        for (stockID,stockName),group in groups:
            df = group.reset_index()
            

            # 计算滚动极差（价格波动范围）
            df['max'] = df["收盘价"].rolling(window=window_size).max()
            df['min'] = df["收盘价"].rolling(window=window_size).min()
            df['range_ratio'] = (df["max"] -  df['min'])/ df['min'] * 100

            # 计算滚动四分位距 (75%分位数 - 25%分位数)
            df['rolling_iqr'] = df["收盘价"].rolling(window=window_size).apply(
                lambda x: np.percentile(x, 75) - np.percentile(x, 25), raw=True
            )

            # 计算滚动四分位距与当前价格的比率（相对波动幅度）
            df['rolling_iqr_ratio'] = df['rolling_iqr'] / df["收盘价"] * 100

            # if stockID == "600105.SH":
            #     print(df)

            if df.shape[0] < 50:
                continue

            df.dropna()
            df['后一天涨跌幅'] = df["涨跌幅"].shift(-1)

            resultDF = df.copy()
            resultDF = resultDF[(resultDF["range_ratio"] <= 10)]
            resultDF = resultDF[(resultDF["rolling_iqr_ratio"] <= 9)]

            if not resultDF.empty:
                for _,row in resultDF.iterrows():
                    data = {}
                    data["股票代码"] = stockID
                    data["股票简称"] = stockName
                    data["日期"] = row["日期"]
                    data["涨跌幅"] = row["后一天涨跌幅"]
                    data["range_ratio"] = row["range_ratio"]
                    data["rolling_iqr_ratio"] = row["rolling_iqr_ratio"]
                    res.append(data)


        return res
    

    def SelectLastDay(self,window_size = 30):
        tradingDays = self.GetTradingDates()
        res = self.GetStockData(tradingDays[-60],window_size=window_size)
        df = pd.DataFrame(res)
        df = df[df['股票简称'].str.match('[\s\S]*(ST|退)+?[\s\S]*') == False]
        df = df[df['股票代码'].str.match('[\s\S]*(BJ|^688|^30)+?[\s\S]*') == False]


        df = df[df["日期"] >= tradingDays[-2]]
        df.sort_values(["股票代码","日期"],ascending=False,inplace=True)
        root = GetStockFolder(tradingDays[-1])
        excleFile = f'''{root}/平地战法{window_size}.xlsx'''

        yestodayDf = df[df["日期"] == tradingDays[-2]]
        todayDf = df[df["日期"] == tradingDays[-1]]
        disappeared_details = df[df['股票代码'].isin(yestodayDf['股票代码'])].copy()
 
        disappeared_details =  df[~df['股票代码'].isin(todayDf['股票代码'])].copy()
        disappeared_details.to_excel(excleFile,index=False)
        disappeared_details = disappeared_details[disappeared_details["涨跌幅"] > 0]
        disappeared_details.sort_values("涨跌幅",ascending=False,inplace=True)
        DataFrameToJPG(disappeared_details,("股票代码","股票简称"),root,f'''平地战法{window_size}''')
        print(df)

    def Select(self,window_size = 30):
        tradingDays = self.GetTradingDates()
        res = self.GetStockData(tradingDays[-60],window_size=window_size)
        df = pd.DataFrame(res)
        df = df[df['股票简称'].str.match('[\s\S]*(ST|退)+?[\s\S]*') == False]
        df = df[df['股票代码'].str.match('[\s\S]*(BJ|^688|^30)+?[\s\S]*') == False]
        df.sort_values("日期",ascending=False,inplace=True)
        df.to_excel('/tmp/平地战法.xlsx',index=False)
        DataFrameToJPG(df,("股票代码","股票简称"),"/tmp/","平地战法")
        print(df)


