import datetime
import pytz
import pandas as pd
from Utility.convertDataFrameToJPG import DataFrameToJPG
from workspace import GetStockFolder
from mysql.connect2DB import DataFrameToSqls_REPLACE
import re

class CStockPattern10(object):
    '''
    倍量战法
    1. 在N 个交易日内放量,{"1日量比":2,"3日量比":1.5,"5日量比":2,"10日量比":2}
    '''
    def __init__(self,dbConnection,lastNDays = 200):
        self.dbConnection = dbConnection
        self.lastNDays = lastNDays
        self.N = 120
        self.threshold = {"1日量比":2,"3日量比":1.5,"5日量比":2,"10日量比":2,"20日量比":2}
        self.liangbis = [1,3,5,10,20]
    
    def GetTradingDates(self):
        today = datetime.datetime.now(pytz.timezone('Asia/Shanghai')).date()
        end = today.strftime("%Y-%m-%d")
        sql = f"SELECT `日期` FROM stock.treadingDay where `开市` =1 and `交易所`='SSE' and `日期`<='{end}' order by `日期` DESC limit {self.lastNDays};"
        res,_ = self.dbConnection.Query(sql)
        self.tradingDays = [r[0] for r in reversed(res)]
        return self.tradingDays

    def FirstVolumnAlarm(self,df,liangbis,threshold:dict):
        newDF = self.FilterByVolumn(df,liangbis,threshold)
        if newDF.empty:
            return (False,None)
        else:
            last = df.iloc[-1]["日期"]
            last1 = newDF.iloc[-1]["日期"]
            res = list(newDF["日期"])
            if last == last1:
                return (True,res)
            
            return (False,res)

    def FilterByVolumn(self,df,liangbis,threshold:dict):
        newDF = pd.DataFrame(df,columns=["日期","收盘价","成交量"])
        newDF["序号"] = newDF.index + 1
        newDF["收盘价"] = newDF["收盘价"].astype("float")
        newDF["成交量"] = newDF["成交量"].astype("float")

        newDF["昨日收盘价"] =  newDF["收盘价"].shift()
        newDF["涨跌幅"] =  (newDF["收盘价"] - newDF["昨日收盘价"])/newDF["昨日收盘价"]*100

        newDF["昨日成交量"] =  newDF["成交量"].shift()

        for liangbi in liangbis:
            key = f'''{liangbi}日均量'''
            key1 = f'''{liangbi}日均量_昨日'''
            key2 = f'''{liangbi}日量比'''
            newDF[key] =  newDF["成交量"].rolling(window=liangbi).mean()
            newDF[key1] =  newDF[key].shift()
            newDF[key2] = newDF["成交量"]/newDF[key1]
            
        for key in threshold:
            value = threshold[key]
            newDF = newDF[newDF[key] >= value]
        
        if not newDF.empty:
            newDF.reset_index(inplace=True,drop=True)
            index0 = newDF.iloc[0]["序号"]
            newDF["第一次放量间隔"] = newDF["序号"] - index0

        return newDF
    
        
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
        flag, res = self.FirstVolumnAlarm(df,self.liangbis,self.threshold)
        if flag == False :
            return None
 
        result = {}
        result["日期"] = today
        result["股票代码"] = stockID
        result["股票名称"] = stockName
        result["战法名称"] = f'''{self.N}日内倍量'''
        # result["今日是否放量"] = flag
        result["放量次数"] = len(res)
        result["第一次放量"] = res[0]
        result["第二次放量"] = ""
        result["第三次放量"] = ""
        result["四次+放量"] = ""

        if len(res) >=2:
            result["第二次放量"] = res[1]

        if len(res) >=3:
            result["第三次放量"] = res[2]      

        if len(res) >=4:
            result["四次+放量"] =  " ; ".join(res[3:])

        return result

    def _FilterAllStocks(self,allDataDf:pd.DataFrame,today):
        groups = allDataDf.groupby(["股票代码","股票名称"])
        results = []
        for (stockID,stockName), group in groups:
            res = self._FilterLast(stockID,stockName,group,today)
            if res is not None:
                results.append(res)
        
        return results
    

    def SelectLast(self):
            days = 120
            tradingDays = self.GetTradingDates()
            tradingDays = tradingDays[-days:]
            df = self._GetStockData(tradingDays[0])
            results = []
            res = self._FilterAllStocks(df,tradingDays[-1])
            results.extend(res)

            resultDf = pd.DataFrame(results)
            if not resultDf.empty:
                resultDf = resultDf[resultDf['股票名称'].str.match('[\s\S]*(ST|退|C)+?[\s\S]*') == False]
                resultDf = resultDf[resultDf['股票代码'].str.match('[\s\S]*(BJ|^688)+?[\s\S]*') == False]
            root = GetStockFolder(tradingDays[-1])
            resultDf.to_excel(f'''{root}/{self.N}日内倍量.xlsx''',index=False)

            # newDF = pd.DataFrame(df, columns = ["日期","股票代码","股票名称","战法名称","买入日期","买入价格"])
            # sqls = DataFrameToSqls_REPLACE(newDF,"zhanfa")
            # sql = " ".join(sqls)
            # self.dbConnection.Execute(sql)

            # sql1 = f'''SELECT `日期`,`股票代码`,`股票名称` FROM stock.zhanfa where `日期` = "{today}" and   `战法名称` = "N形战法" and `股票代码` not in (SELECT `股票代码` FROM stock.zhanfa where `日期` = "{yesterday}")'''
            # result1,columns1 = self.dbConnection.Query(sql1)
            # newDf1=pd.DataFrame(result1,columns=columns1)

            #DataFrameToJPG(newDf1,("股票代码","股票名称"),root,"N形战法_增量")
            DataFrameToJPG(resultDf,("股票代码","股票名称"),root,f'''{self.N}日内倍量''')
            print(resultDf)

    def SelectAll(self):
            days = 120
            tradingDays = self.GetTradingDates()
            tradingDays = tradingDays[-days:]
            df = self._GetStockData(tradingDays[0])
            results = []
            for index in range(1,days - self.N):
                today = tradingDays[-index]
                print(f'''{index}, {today}, total:{days - self.N}''')
                newDF = df[df["日期"] <= today]
                res = self._FilterAllStocks(newDF,today)
                results.extend(res)

            resultDf = pd.DataFrame(results)
            if not resultDf.empty:
                resultDf = resultDf[resultDf['股票名称'].str.match('[\s\S]*(ST|退|C)+?[\s\S]*') == False]
                resultDf = resultDf[resultDf['股票代码'].str.match('[\s\S]*(BJ|^688)+?[\s\S]*') == False]
            root = GetStockFolder(tradingDays[-1])
            resultDf.to_excel(f'''{root}/{self.N}日内倍量.xlsx''',index=False)

            # newDF = pd.DataFrame(df, columns = ["日期","股票代码","股票名称","战法名称","买入日期","买入价格"])
            # sqls = DataFrameToSqls_REPLACE(newDF,"zhanfa")
            # sql = " ".join(sqls)
            # self.dbConnection.Execute(sql)

            # sql1 = f'''SELECT `日期`,`股票代码`,`股票名称` FROM stock.zhanfa where `日期` = "{today}" and   `战法名称` = "N形战法" and `股票代码` not in (SELECT `股票代码` FROM stock.zhanfa where `日期` = "{yesterday}")'''
            # result1,columns1 = self.dbConnection.Query(sql1)
            # newDf1=pd.DataFrame(result1,columns=columns1)

            #DataFrameToJPG(newDf1,("股票代码","股票名称"),root,"N形战法_增量")
            DataFrameToJPG(resultDf,("股票代码","股票名称"),root,f'''{self.N}日内倍量''')
            print(resultDf)

    # def GetStockData(self,startDay,today):
    #     sql1 = f'''
    #     SELECT B.`日期`,A.`股票代码`,A.`股票简称`,B.`开盘价`,B.`收盘价` ,B.`最低价`,B.`最高价`,B.`成交量` FROM stock.stockbasicinfo AS A,(SELECT `日期`,`股票代码`,`开盘价`,`收盘价`,`最低价`,`最高价`,`成交量` FROM stock.stockdailyinfo_2023 where `日期` >= "{startDay}" UNION SELECT `日期`,`股票代码`,`开盘价`,`收盘价` ,`最低价`,`最高价`,`成交量` FROM stock.stockdailyinfo where `日期` >= "{startDay}") AS B where A.`股票代码` = B.`股票代码`
    #     '''
    #     result1,columns1 = self.dbConnection.Query(sql1)
    #     newDf1=pd.DataFrame(result1,columns=columns1)
    #     groups = newDf1.groupby(["股票代码","股票简称"])
    #     res = []
    #     for stockID, group in groups:
    #         df = group.reset_index(drop=True)
    #         if df.shape[0] <= 20:
    #             continue
    #         # if stockID[0] == "000421.SZ":
    #         #     print(stockID)
    #         df.dropna(inplace=True)
    #         df["开盘价"] = df["开盘价"].astype("float")
    #         df["收盘价"] = df["收盘价"].astype("float")
    #         df["最低价"] = df["最低价"].astype("float")
    #         df["最高价"] = df["最高价"].astype("float")
    #         df["成交量"] = df["成交量"].astype("float")
    #         df["昨日收盘价"] = df["收盘价"].shift()
    #         df["MA5"] = df["收盘价"].rolling(window=5).mean()
    #         df["MA10"] = df["收盘价"].rolling(window=10).mean()
    #         df["MA20"] = df["收盘价"].rolling(window=20).mean()
    #         df["VMA5"] = df["成交量"].rolling(window=5).mean()
    #         df["量/VMA5"] = df["成交量"]/df["VMA5"]
    #         df["是否阳线"] = df["收盘价"] >= df["开盘价"]
    #         df["涨跌幅"] = (df["收盘价"] - df["昨日收盘价"])/df["昨日收盘价"] * 100
    #         df["是否大涨"] = False
    #         df["是否大跌"] = False
    #         if re.match('^00.*|^60.*',stockID[0]) is not None:
    #             df["是否大涨"] = df["涨跌幅"] >= 7
    #             df["是否大跌"] = df["涨跌幅"] <= -5
    #         elif re.match('^68.*|^30.*',stockID[0]) is not None:
    #             df["是否大涨"] = df["涨跌幅"] >= 7
    #             df["是否大跌"] = df["涨跌幅"] <= -7
                
    #         resRow = self._ProcessFlag(df)
    #         if resRow is None:
    #             continue

    #         day = resRow["日期"]
    #         startPrice = resRow["开盘价"]
    #         newDf = df[df["日期"] >= day]
    #         zhangDieFu = float(f'''{(newDf.iloc[-1]["收盘价"] - startPrice) / startPrice * 100:.2f}''')

    #         if zhangDieFu >3: 
    #             continue

    #         beforeRow = df[df["日期"] < resRow["日期"]].iloc[-1]


    #         result = {}
    #         result["日期"] = today
    #         result["股票代码"] = stockID[0]
    #         result["股票名称"] = stockID[1]
    #         result["战法名称"] = f'''{self.N}日内放量大阳线'''
    #         result["起始价"] = startPrice
    #         result["起始日期"] = day

    #         result["起始日之前是否大涨"] = False
    #         if beforeRow["是否大涨"] == True:
    #             result["起始日之前是否大涨"] = True

    #         result["起涨日日今涨幅"] = zhangDieFu
    #         result["起始距今天数"] = newDf.shape[0]

    #         res.append(result)

    #     return res
    
    # def Select(self):
    #     tradingDays = self.GetTradingDates()
    #     tradingDays = tradingDays[-90:]
    #     today = tradingDays[-1]
    #     res = self.GetStockData(tradingDays[0],today)
    #     df = pd.DataFrame(res)
    #     df = df[df['股票名称'].str.match('[\s\S]*(ST|退|C)+?[\s\S]*') == False]
    #     df = df[df['股票代码'].str.match('[\s\S]*(BJ|^688)+?[\s\S]*') == False]
    #     root = GetStockFolder(tradingDays[-1])
    #     df.to_excel(f'''{root}/{self.N}日内放量大阳线.xlsx''',index=False)

    #     # newDF = pd.DataFrame(df, columns = ["日期","股票代码","股票名称","战法名称","买入日期","买入价格"])
    #     # sqls = DataFrameToSqls_REPLACE(newDF,"zhanfa")
    #     # sql = " ".join(sqls)
    #     # self.dbConnection.Execute(sql)

    #     # sql1 = f'''SELECT `日期`,`股票代码`,`股票名称` FROM stock.zhanfa where `日期` = "{today}" and   `战法名称` = "N形战法" and `股票代码` not in (SELECT `股票代码` FROM stock.zhanfa where `日期` = "{yesterday}")'''
    #     # result1,columns1 = self.dbConnection.Query(sql1)
    #     # newDf1=pd.DataFrame(result1,columns=columns1)

    #     #DataFrameToJPG(newDf1,("股票代码","股票名称"),root,"N形战法_增量")
    #     DataFrameToJPG(df,("股票代码","股票名称"),root,f'''{self.N}日内放量大阳线''')
    #     print(df)

