import datetime
import pytz
import pandas as pd
from Utility.convertDataFrameToJPG import DataFrameToJPG

class CPercentilePattern(object):
    '''
    百分位选股器: 股价现在占现最近N个交易日的百分位
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

    def GetStockData(self,startDay, percentail):
        sql1 = f'''
        SELECT B.`日期`,A.`股票代码`,A.`股票简称`,B.`收盘价`,B.`开盘价`,B.`最高价`,B.`最低价`,B.`涨跌幅`,B.`成交量` FROM stock.stockbasicinfo AS A,(SELECT `日期`,`股票代码`,`开盘价`,`收盘价`,`最高价`,`最低价`,`涨跌幅`,`成交量` FROM stock.stockdailyinfo_2024 where `日期` >= "{startDay}" UNION SELECT `日期`,`股票代码`,`开盘价`,`收盘价`,`最高价`,`最低价`,`涨跌幅`,`成交量` FROM stock.stockdailyinfo where `日期` >= "{startDay}") AS B where A.`股票代码` = B.`股票代码`
        '''
        result1,columns1 = self.dbConnection.Query(sql1)
        newDf1=pd.DataFrame(result1,columns=columns1)
        newDf1["开盘价"] = newDf1["开盘价"].astype("float").round(3)
        newDf1["收盘价"] = newDf1["收盘价"].astype("float").round(3)
        newDf1["最高价"] = newDf1["最高价"].astype("float").round(3)
        newDf1["最低价"] = newDf1["最低价"].astype("float").round(3)
        groups = newDf1.groupby(["股票代码","股票简称"])
        res = []
        for stockID, group in groups:
            df = group.reset_index()
            
            df.dropna()


            df['percentile_rank'] = df['最低价'].rank(method='min', pct=True)
            df['percentile'] = df['percentile_rank'] 

            lastLine = df.iloc[-1]


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
    
    def Select(self,percentail = [0.2,0.8]):
        tradingDays = self.GetTradingDates()
        res = self.GetStockData(tradingDays[-300])
        df = pd.DataFrame(res,columns = ["股票代码","股票简称","结果"])
        df = df[df['股票简称'].str.match('[\s\S]*(ST|退)+?[\s\S]*') == False]
        df = df[df['股票代码'].str.match('[\s\S]*(BJ|^688|^30)+?[\s\S]*') == False]
        df.sort_values("结果",ascending=False,inplace=True)
        df.to_excel('/tmp/百分位选股器.xlsx',index=False)
        DataFrameToJPG(df,("股票代码","股票简称"),"/tmp/","百分位选股器")
        print(df)


