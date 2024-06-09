import datetime
import pytz
import pandas as pd
from Utility.convertDataFrameToJPG import DataFrameToJPG
from workspace import GetStockFolder
from mysql.connect2DB import DataFrameToSqls_REPLACE

class CStockPattern5(object):
    '''
    N 形战法
    1. 重心过前三
    2. 量过前三
    3. 站上20日线
    4. 洗盘不超过20个交易日,所以前20 个交易日内有涨停，并且站上 20日线
    5. 不破主力成本线，即第一个涨停板的开盘价
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

    def _processStepOne(self,df:pd.DataFrame,N = 30):
        #1.有涨停 用涨跌幅大于9 个点来代替
        #2.涨停当日的成交量大于2日成交量的 2倍以上，5倍以下
        #3.涨停当日站上20日均线
        row_fined = None

        if df.shape[0]< N:
            return row_fined
        

        rangeDf = df[-N:]
        ZhangTingDf = rangeDf[rangeDf["涨跌幅"] >= 9]
        if ZhangTingDf.shape[0] == 0:
            return row_fined

        for _, row in ZhangTingDf.iterrows():
            v = row["成交量"] / row["VMA5"]
            k = row["收盘价"] > row["MA20"]
            o = row["收盘价"] >= row["开盘价"]
            if v >=2 and v<=5 and k and o:
                row_fined = row
                break
            
        return row_fined
    
    def _ProcessBuyCondition(self,df):
        count = df.shape[0]
        row_finded = None
        if count <=4:
            return row_finded
        
        for index in range(3,count):
            last3 = df.iloc[index-3]
            last2 = df.iloc[index-2]
            last1 = df.iloc[index-1]
            last = df.iloc[index]
            res1 = last["重心"] > max(last1["重心"],last2["重心"],last3["重心"])
            res2 = last["成交量"] > max(last1["成交量"],last2["成交量"],last3["成交量"])
            row_finded = last
            if res1 and res2:
                return row_finded
        
        return row_finded
    
    def _ProcessBuyConditionProdict(self,df): #预测
        count = df.shape[0]
        if count <=4:
            return ("","")
        
        last2 = df.iloc[-3]
        last1 = df.iloc[-2]
        last = df.iloc[-1]
        res1 = max(last["重心"],last1["重心"],last2["重心"])
        res2 = max(last["成交量"],last1["成交量"],last2["成交量"])
        return (res1,res2)

    def GetStockData(self,startDay,today):
        sql1 = f'''
        SELECT B.`日期`,A.`股票代码`,A.`股票简称`,B.`开盘价`,B.`收盘价` ,B.`最低价`,B.`成交量` FROM stock.stockbasicinfo AS A,(SELECT `日期`,`股票代码`,`开盘价`,`收盘价`,`最低价`,`成交量` FROM stock.stockdailyinfo_2023 where `日期` >= "{startDay}" UNION SELECT `日期`,`股票代码`,`开盘价`,`收盘价` ,`最低价`,`成交量` FROM stock.stockdailyinfo where `日期` >= "{startDay}") AS B where A.`股票代码` = B.`股票代码`
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
            df["开盘价"] = df["开盘价"].astype("float")
            df["收盘价"] = df["收盘价"].astype("float")
            df["最低价"] = df["最低价"].astype("float")
            df["成交量"] = df["成交量"].astype("float")
            df["昨日收盘价"] = df["收盘价"].shift()
            df["MA20"] = df["收盘价"].rolling(window=20).mean()
            df["VMA5"] = df["成交量"].rolling(window=5).mean()
            df["涨跌幅"] = (df["收盘价"] - df["昨日收盘价"])/df["昨日收盘价"] * 100
            df["重心"] =  (df["收盘价"] + df["收盘价"] ) / 2
            resRow = self._processStepOne(df)
            if resRow is None:
                continue
            
            # print(stockID,resRow)
            day = resRow["日期"]
            startPrice = resRow["开盘价"]

            newDf = df[df["日期"] >= day]
            if newDf.iloc[-1]["收盘价"] < startPrice:
                continue

            broken = newDf[newDf["收盘价"]<startPrice]
            
            result = {}
            result["日期"] = today
            result["股票代码"] = stockID[0]
            result["股票名称"] = stockID[1]
            result["战法名称"] = "N形战法"
            result["建仓价"] = startPrice
            result["建仓日期"] = day
            result["建仓距今涨幅"] = float(f'''{(newDf.iloc[-1]["收盘价"] - startPrice) / startPrice * 100:.2f}''')
            result["建仓距今最大涨幅"] = float(f'''{(newDf["收盘价"].max() - startPrice) / startPrice * 100:.2f}''')
            result["建仓距今最小涨幅"] = float(f'''{(newDf["收盘价"].min() - startPrice) / startPrice * 100:.2f}''')
            result["距今交易日天数"] = newDf.shape[0]
            result["买入日期"] = ""
            result["买入价格"] = ""
            result["明日达标重心"] = ""
            result["明日达标成交量"] = ""
            result["跌破建仓价"] = False
            result["跌破建仓价日期"] = ""
        
            brokenDays = []
            if broken.shape[0] >0:
                brokenDays = list(broken["日期"])
                result["跌破建仓价"] = True
            
            result["跌破建仓价日期"] = ";".join(brokenDays)
            buyRow = self._ProcessBuyCondition(newDf)
            
            if buyRow is not None:
                result["买入日期"] = buyRow["日期"]
                result["买入价格"] = buyRow["收盘价"]
            else:
                prodict = self._ProcessBuyConditionProdict(df)
                result["明日达标重心"] = prodict[0]
                result["明日达标成交量"] = prodict[1]

            res.append(result)

        return res
    
    def Select(self):
        tradingDays = self.GetTradingDates()
        tradingDays = tradingDays[-90:]
        today = tradingDays[-1]
        yesterday = tradingDays[-2]
        res = self.GetStockData(tradingDays[0],today)
        df = pd.DataFrame(res)
        df.sort_values("建仓距今涨幅",ascending=True,inplace=True)
        df = df[df['股票名称'].str.match('[\s\S]*(ST|退|C)+?[\s\S]*') == False]
        df = df[df['股票代码'].str.match('[\s\S]*(BJ|^688)+?[\s\S]*') == False]
        root = GetStockFolder(tradingDays[-1])
        df.to_excel(f'''{root}/N形战法.xlsx''',index=False)

        newDF = pd.DataFrame(df, columns = ["日期","股票代码","股票名称","战法名称","买入日期","买入价格"])
        sqls = DataFrameToSqls_REPLACE(newDF,"zhanfa")
        sql = " ".join(sqls)
        self.dbConnection.Execute(sql)

        sql1 = f'''SELECT `日期`,`股票代码`,`股票名称` FROM stock.zhanfa where `日期` = "{today}" and   `战法名称` = "N形战法" and `股票代码` not in (SELECT `股票代码` FROM stock.zhanfa where `日期` = "{yesterday}")'''
        result1,columns1 = self.dbConnection.Query(sql1)
        newDf1=pd.DataFrame(result1,columns=columns1)

        DataFrameToJPG(newDf1,("股票代码","股票名称"),root,"N形战法_增量")
        DataFrameToJPG(df,("股票代码","股票名称"),root,"N形战法")
        print(newDf1)

