import datetime
import pytz
import pandas as pd
from Utility.convertDataFrameToJPG import DataFrameToJPG

class CPattern13(object):
    '''
    N型选股器: 红三兵，再黑三兵，例如： 川润股份 2024年10月10日
    '''
    def __init__(self,dbConnection,lastNDays = 500):
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
        SELECT B.`日期`,A.`股票代码`,A.`股票简称`,B.`收盘价`,B.`开盘价`,B.`涨跌幅`,B.`成交量` FROM stock.stockbasicinfo AS A,(SELECT `日期`,`股票代码`,`开盘价`,`收盘价`,`涨跌幅`,`成交量` FROM stock.stockdailyinfo_2024 where `日期` >= "{startDay}" UNION SELECT `日期`,`股票代码`,`开盘价`,`收盘价`,`涨跌幅`,`成交量` FROM stock.stockdailyinfo where `日期` >= "{startDay}") AS B where A.`股票代码` = B.`股票代码`
        '''
        result1,columns1 = self.dbConnection.Query(sql1)
        newDf1=pd.DataFrame(result1,columns=columns1)
        newDf1["开盘价"] = newDf1["开盘价"].astype("float").round(3)
        newDf1["收盘价"] = newDf1["收盘价"].astype("float").round(3)
        newDf1["涨跌幅"] = newDf1["涨跌幅"].astype("float").round(3)
        newDf1["成交量"] = newDf1["成交量"].astype("float").round(3)
        newDf1["是否阴线"] = newDf1["收盘价"] < newDf1["开盘价"]


        groups = newDf1.groupby(["股票代码","股票简称"])
        res = []
        for stockID, group in groups:
            df = group.reset_index()
            
            if stockID[0] == "002272.SZ":
                print(df)
        
            df["1天前否阴线"] = df['是否阴线'].shift(1,fill_value=False)
            df["2天前否阴线"] = df['是否阴线'].shift(2,fill_value=False)
            df["3天前否阴线"] = df['是否阴线'].shift(3,fill_value=False)
            df["4天前否阴线"] = df['是否阴线'].shift(4,fill_value=False)
            df["5天前否阴线"] = df['是否阴线'].shift(5,fill_value=False)


            df["1天前成交量"] = df['成交量'].shift(1,fill_value=None)
            df["2天前成交量"] = df['成交量'].shift(2,fill_value=None)
            df["3天前成交量"] = df['成交量'].shift(3,fill_value=None)
            df["4天前成交量"] = df['成交量'].shift(4,fill_value=None)
            df["5天前成交量"] = df['成交量'].shift(5,fill_value=None)


            df["1天前收盘价"] = df['收盘价'].shift(1,fill_value=None)
            df["2天前收盘价"] = df['收盘价'].shift(2,fill_value=None)
            df["3天前收盘价"] = df['收盘价'].shift(3,fill_value=None)
            df["4天前收盘价"] = df['收盘价'].shift(4,fill_value=None)
            df["5天前收盘价"] = df['收盘价'].shift(5,fill_value=None)

            df["3天前涨跌幅"] = df['涨跌幅'].shift(3,fill_value=None)
            df.dropna()


            resultDF = df[(df["是否阴线"] == True)]
            resultDF = resultDF[resultDF["1天前否阴线"] == True]
            resultDF = resultDF[resultDF["2天前否阴线"] == True]
            resultDF = resultDF[resultDF["3天前否阴线"] == False]
            resultDF = resultDF[resultDF["4天前否阴线"] == False]
            resultDF = resultDF[resultDF["5天前否阴线"] == False]


            resultDF = resultDF[(resultDF["4天前成交量"]>= 1.2*resultDF["5天前成交量"]) &  (resultDF["4天前成交量"]<= 2*resultDF["5天前成交量"])]
            resultDF = resultDF[(resultDF["3天前成交量"]>= 1.2*resultDF["4天前成交量"]) &  (resultDF["3天前成交量"]<= 2*resultDF["4天前成交量"])]

            resultDF = resultDF[(resultDF["2天前成交量"]>= 0.8*resultDF["3天前成交量"]) &  (resultDF["2天前成交量"]<= 1.5*resultDF["3天前成交量"])]


            resultDF = resultDF[(resultDF["2天前成交量"]>= 1.2*resultDF["1天前成交量"]) &  (resultDF["2天前成交量"]<= 2*resultDF["1天前成交量"])]
            resultDF = resultDF[(resultDF["1天前成交量"]>= 1.2*resultDF["成交量"]) &  (resultDF["1天前成交量"]<= 2*resultDF["成交量"])]

            resultDF = resultDF[resultDF["3天前涨跌幅"] >= 9]

            if not resultDF.empty:
                data = []
                data.extend(stockID)
                t = "; ".join(reversed(list(resultDF["日期"])))
                data.append(t)
                res.append(data)

        return res
    
    def Select(self):
        tradingDays = self.GetTradingDates()
        res = self.GetStockData(tradingDays[-300])
        df = pd.DataFrame(res,columns = ["股票代码","股票简称","结果"])
        df = df[df['股票简称'].str.match('[\s\S]*(ST|退)+?[\s\S]*') == False]
        df = df[df['股票代码'].str.match('[\s\S]*(BJ|^688|^30)+?[\s\S]*') == False]
        df.sort_values("结果",ascending=False,inplace=True)
        df.to_excel('/tmp/红三兵-黑三兵.xlsx',index=False)
        DataFrameToJPG(df,("股票代码","股票简称"),"/tmp/","红三兵-黑三兵")
        print(df)


