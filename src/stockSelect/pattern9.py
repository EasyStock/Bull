import datetime
import pytz
import pandas as pd
from Utility.convertDataFrameToJPG import DataFrameToJPG
from workspace import GetStockFolder
from mysql.connect2DB import DataFrameToSqls_REPLACE
import re
import numpy as np

class CStockPattern9(object):
    '''
    放量大阳线
    1. 在N 个交易日内放量(成交量必须是5日量的1.75倍以上), 必须是第一次出现这个情况，之前出现就无效
    2. 大阳线
    均线粘合
    '''
    def __init__(self,dbConnection,lastNDays = 200):
        self.dbConnection = dbConnection
        self.lastNDays = lastNDays
        self.N = 30
        self.liangBi = 1.75
        self.stdMA = 1.5
    
    def GetTradingDates(self):
        today = datetime.datetime.now(pytz.timezone('Asia/Shanghai')).date()
        end = today.strftime("%Y-%m-%d")
        sql = f"SELECT `日期` FROM stock.treadingDay where `开市` =1 and `交易所`='SSE' and `日期`<='{end}' order by `日期` DESC limit {self.lastNDays};"
        res,_ = self.dbConnection.Query(sql)
        self.tradingDays = [r[0] for r in reversed(res)]
        return self.tradingDays

    def _direction(self,row:pd.Series):
        #针对慢性的均线判断方向
        result = 0
        for index in range(1,4):
            if row.iloc[-index-1] - row.iloc[-index] > 0:
                result = result - 1
            elif row.iloc[-index-1] - row.iloc[-index] < 0:
                result = result + 1
        
        if result ==0:
            return "未知"
        elif result <=0:
            return "下"
        else:
            return "上"

    def _ProcessFlag(self,df:pd.DataFrame):
        # 最近N个交易日内有以下条件:
        # 0. 阳线
        # 1. 10CM 涨停的票有7% 以上涨幅
        # 2. 20CM 涨停的票有9% 以上涨幅
        # 3. 5日量比大于1.75倍
        row_not_fined = None
        if df.shape[0]< self.N:
            return row_not_fined
    
        rangeDf = df[-self.N:]

        lastRow = rangeDf.iloc[-1]
        flag1  = (lastRow["是否大涨"] == True) and  (lastRow["是否阳线"] == True) and (lastRow["MA5"] >= lastRow["MA10"]) and (lastRow["MA10"] >= lastRow["MA20"])
        flag2= lastRow["量/VMA5"] >=self.liangBi
        # flag3 = lastRow["开盘价"] == lastRow["最高价"] #不能一字板开盘
        if (not flag1) or (not flag2):
            return row_not_fined
    
        beforeDF = rangeDf[rangeDf["日期"] < lastRow["日期"]]
        newdf = beforeDF[beforeDF["成交量"] >= 0.8*lastRow["成交量"]]
        if not newdf.empty:  # 找到这个成交量，必须在 self.N 之内，row1 出现之前，没有出现过这么大的量
            return row_not_fined
        
        newdf1 = beforeDF[beforeDF["是否大涨"]]
        if not newdf1.empty:  # 找到这个成交量，必须在 self.N 之内，row1 出现之前，没有出现过这么大的量
            return row_not_fined
        
        # 均线粘合
        a1 = [lastRow["开盘价"],lastRow["MA5"],lastRow["MA10"],lastRow["MA20"],lastRow["MA30"],lastRow["MA60"],lastRow["MA120"]]
        a2 = [lastRow["开盘价"],lastRow["MA5"],lastRow["MA10"],lastRow["MA20"],lastRow["MA30"],lastRow["MA60"],lastRow["MA120"],lastRow["MA200"]]
        # if np.std(a) > self.stdMA:
        #     return row_not_fined
        returnRow = lastRow.copy()
        returnRow["均线标准差1"] = np.std(a1)
        returnRow["均线标准差2"] = np.std(a2)
        ma120Dir = self._direction(beforeDF["MA120"])
        ma200Dir = self._direction(beforeDF["MA200"])
        returnRow["MA120方向"] = ma120Dir
        returnRow["MA200方向"] = ma200Dir
        return returnRow
        
    def _GetStockData(self,startDay):
        sql1 = f'''
        SELECT B.`日期`,A.`股票代码`,A.`股票简称` As `股票名称` ,B.`开盘价`,B.`收盘价` ,B.`最低价`,B.`最高价`,B.`成交量` FROM stock.stockbasicinfo AS A,(SELECT `日期`,`股票代码`,`开盘价`,`收盘价`,`最低价`,`最高价`,`成交量` FROM stock.stockdailyinfo_2023 where `日期` >= "{startDay}" UNION SELECT `日期`,`股票代码`,`开盘价`,`收盘价` ,`最低价`,`最高价`,`成交量` FROM stock.stockdailyinfo where `日期` >= "{startDay}") AS B where A.`股票代码` = B.`股票代码`
        '''
        result1,columns1 = self.dbConnection.Query(sql1)
        newDf1=pd.DataFrame(result1,columns=columns1)
        return newDf1
    
    def _FilterLast(self,stockID,stockName,df:pd.DataFrame,today):
        df = df.reset_index(drop=True)
        if df.shape[0] <= 20:
            return None
    
        df.dropna(inplace=True)
        df["开盘价"] = df["开盘价"].astype("float")
        df["收盘价"] = df["收盘价"].astype("float")
        df["最低价"] = df["最低价"].astype("float")
        df["最高价"] = df["最高价"].astype("float")
        df["成交量"] = df["成交量"].astype("float")
        df["昨日收盘价"] = df["收盘价"].shift()
        df["MA5"] = df["收盘价"].rolling(window=5).mean()
        df["MA10"] = df["收盘价"].rolling(window=10).mean()
        df["MA20"] = df["收盘价"].rolling(window=20).mean()
        df["MA30"] = df["收盘价"].rolling(window=30).mean()
        df["MA60"] = df["收盘价"].rolling(window=60).mean()
        df["MA120"] = df["收盘价"].rolling(window=120).mean()
        df["MA200"] = df["收盘价"].rolling(window=200).mean()
        df["MA5_10距离"] = (df["MA5"] - df["MA10"])**2 
        df["MA10_20距离"] =  (df["MA10"] - df["MA20"])**2
        df["MA20_30距离"] = (df["MA20"] - df["MA30"])**2
        df["MA30_60距离"] = (df["MA30"] - df["MA60"])**2
        df["MA60_120距离"] = (df["MA60"] - df["MA120"])**2
        df["MA120_200距离"] = (df["MA120"] - df["MA200"])**2
        df["MA短"] = df["MA5_10距离"]  + df["MA10_20距离"]
        df["MA中"] = df["MA短"] + df["MA20_30距离"] + df["MA30_60距离"]
        df["MA长"] = df["MA中"] + df["MA60_120距离"] + df["MA120_200距离"]

        df["VMA5"] = df["成交量"].rolling(window=5).mean()
        df["量/VMA5"] = df["成交量"]/df["VMA5"]
        df["是否阳线"] = df["收盘价"] >= df["开盘价"]
        df["涨跌幅"] = (df["收盘价"] - df["昨日收盘价"])/df["昨日收盘价"] * 100
        df["是否大涨"] = False
        df["是否大跌"] = False
        if re.match('^00.*|^60.*',stockID) is not None:
            df["是否大涨"] = df["涨跌幅"] >= 7
            df["是否大跌"] = df["涨跌幅"] <= -5
        elif re.match('^68.*|^30.*',stockID) is not None:
            df["是否大涨"] = df["涨跌幅"] >= 7
            df["是否大跌"] = df["涨跌幅"] <= -7
                
        resRow = self._ProcessFlag(df)
        if resRow is None:
            return None

        day = resRow["日期"]
        startPrice = resRow["开盘价"]

        result = {}
        result["日期"] = today
        result["股票代码"] = stockID
        result["股票名称"] = stockName
        result["战法名称"] = f'''{self.N}日内放量大阳线'''
        result["起始价"] = startPrice
        result["起始日期"] = day
        result["均线标准差1"] = resRow["均线标准差1"]
        result["均线标准差2"] = resRow["均线标准差2"]
        result["MA120方向"] = resRow["MA120方向"]
        result["MA200方向"] = resRow["MA200方向"]
        return result

    def _FilterAllStocks(self,allDataDf:pd.DataFrame,today):
        groups = allDataDf.groupby(["股票代码","股票名称"])
        results = []
        for (stockID,stockName), group in groups:
            if stockID == "300492.SZ":
                print(stockID)
            res = self._FilterLast(stockID,stockName,group,today)
            if res is not None:
                results.append(res)
        
        return results
    

    def SelectAll(self):
            days = 500
            self.lastNDays = days
            tradingDays = self.GetTradingDates()
            tradingDays = tradingDays[-days:]
            df = self._GetStockData(tradingDays[0])
            results = []
            try:
                for index in range(1,100):
                    today = tradingDays[-index]
                    print(f'''{index}, {today}, total:{days - self.N}''')
                    newDF = df[df["日期"] <= today]
                    res = self._FilterAllStocks(newDF,today)
                    results.extend(res)
            except:
                pass
            resultDf = pd.DataFrame(results)
            if not resultDf.empty:
                resultDf = resultDf[resultDf['股票名称'].str.match('[\s\S]*(ST|退|C)+?[\s\S]*') == False]
                resultDf = resultDf[resultDf['股票代码'].str.match('[\s\S]*(BJ|^688)+?[\s\S]*') == False]
            root = GetStockFolder(tradingDays[-1])
            resultDf.to_excel(f'''{root}/{self.N}日内放量大阳线_均线粘合.xlsx''',index=False)

            # newDF = pd.DataFrame(df, columns = ["日期","股票代码","股票名称","战法名称","买入日期","买入价格"])
            # sqls = DataFrameToSqls_REPLACE(newDF,"zhanfa")
            # sql = " ".join(sqls)
            # self.dbConnection.Execute(sql)

            # sql1 = f'''SELECT `日期`,`股票代码`,`股票名称` FROM stock.zhanfa where `日期` = "{today}" and   `战法名称` = "N形战法" and `股票代码` not in (SELECT `股票代码` FROM stock.zhanfa where `日期` = "{yesterday}")'''
            # result1,columns1 = self.dbConnection.Query(sql1)
            # newDf1=pd.DataFrame(result1,columns=columns1)

            #DataFrameToJPG(newDf1,("股票代码","股票名称"),root,"N形战法_增量")
            DataFrameToJPG(resultDf,("股票代码","股票名称"),root,f'''{self.N}日内放量大阳线_均线粘合''')
            print(resultDf)


