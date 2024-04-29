import datetime
import pytz
import pandas as pd
from Utility.convertDataFrameToJPG import DataFrameToJPG
from workspace import GetStockFolder
from mysql.connect2DB import DataFrameToSqls_REPLACE
import re

class CStockPattern7(object):
    '''
    7日放量吸筹战法，与CStockPattern6 类似，但是条件会比 CStockPattern6 更加严苛，目的是找出7日内有放量大涨的票，然后回调
    1. 先放量涨停，
    2. 距离放量大涨那日的起涨点，涨幅可以是 (绝对值3% 以内， 绝对值1% 以内，跌破起涨点)
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

    def _ProcessFlag(self,df:pd.DataFrame,N = 30):
        # 最近N个交易日内有以下条件:
        # 0. 阳线
        # 1. 10CM 涨停的票有7% 以上涨幅
        # 2. 20CM 涨停的票有9% 以上涨幅
        # 3. 5日量比大于1.5
        # 4. 站上20日均线
        # 5. 找到的起始日之前没有涨停
        row_fined = None
        if df.shape[0]< N:
            return row_fined
    
        rangeDf = df[-N:]
        ZhangTingDf = rangeDf[rangeDf["是否大涨"] == True]
        if ZhangTingDf is None or (ZhangTingDf.shape[0] == 0):
            return row_fined

        for _, row in ZhangTingDf.iterrows():
            v = row["成交量"] / row["VMA5"]
            k = row["收盘价"] > row["MA20"]
            o = row["收盘价"] >= row["开盘价"]
            if v >=1.75 and k and o:
                row_fined = row
                break
        
        return row_fined
    


    def GetStockData(self,startDay,today):
        sql1 = f'''
        SELECT B.`日期`,A.`股票代码`,A.`股票简称`,B.`开盘价`,B.`收盘价` ,B.`最低价`,B.`成交量` FROM stock.stockbasicinfo AS A,(SELECT `日期`,`股票代码`,`开盘价`,`收盘价`,`最低价`,`成交量` FROM stock.stockdailyinfo_2023 where `日期` >= "{startDay}" UNION SELECT `日期`,`股票代码`,`开盘价`,`收盘价` ,`最低价`,`成交量` FROM stock.stockdailyinfo where `日期` >= "{startDay}") AS B where A.`股票代码` = B.`股票代码`
        '''
        result1,columns1 = self.dbConnection.Query(sql1)
        newDf1=pd.DataFrame(result1,columns=columns1)
        groups = newDf1.groupby(["股票代码","股票简称"])
        res = []
        for stockID, group in groups:
            df = group.reset_index(drop=True)
            if df.shape[0] <= 20:
                continue
            # if stockID[0] == "300887.SZ":
            #     print(stockID)
            df.dropna(inplace=True)
            df["开盘价"] = df["开盘价"].astype("float")
            df["收盘价"] = df["收盘价"].astype("float")
            df["最低价"] = df["最低价"].astype("float")
            df["成交量"] = df["成交量"].astype("float")
            df["昨日收盘价"] = df["收盘价"].shift()
            df["MA20"] = df["收盘价"].rolling(window=20).mean()
            df["VMA5"] = df["成交量"].rolling(window=5).mean()
            df["涨跌幅"] = (df["收盘价"] - df["昨日收盘价"])/df["昨日收盘价"] * 100
            df["是否大涨"] = False
            df["是否大跌"] = False
            if re.match('^00.*|^60.*',stockID[0]) is not None:
                df["是否大涨"] = df["涨跌幅"] >= 7
                df["是否大跌"] = df["涨跌幅"] <= -5
            elif re.match('^68.*|^30.*',stockID[0]) is not None:
                df["是否大涨"] = df["涨跌幅"] >= 7
                df["是否大跌"] = df["涨跌幅"] <= -7
                
            resRow = self._ProcessFlag(df,N=7)  #7 日之内
            if resRow is None:
                continue

            day = resRow["日期"]
            startPrice = resRow["开盘价"]
            newDf = df[df["日期"] >= day]
            zhangDieFu = float(f'''{(newDf.iloc[-1]["收盘价"] - startPrice) / startPrice * 100:.2f}''')

            if zhangDieFu >3: 
                continue

            beforeRow = df[df["日期"] < resRow["日期"]].iloc[-1]


            result = {}
            result["日期"] = today
            result["股票代码"] = stockID[0]
            result["股票名称"] = stockID[1]
            result["战法名称"] = "7日放量吸筹战法"
            result["起始价"] = startPrice
            result["起始日期"] = day

            result["起始日之前是否大涨"] = False
            if beforeRow["是否大涨"] == True:
                result["起始日之前是否大涨"] = True

            result["起涨日日今涨幅"] = zhangDieFu
            result["起始距今天数"] = newDf.shape[0]

            res.append(result)

        return res
    
    def Select(self):
        tradingDays = self.GetTradingDates()
        tradingDays = tradingDays[-90:]
        today = tradingDays[-1]
        res = self.GetStockData(tradingDays[0],today)
        df = pd.DataFrame(res)
        df = df[df['股票名称'].str.match('[\s\S]*(ST|退|C)+?[\s\S]*') == False]
        df = df[df['股票代码'].str.match('[\s\S]*(BJ|^688)+?[\s\S]*') == False]
        root = GetStockFolder(tradingDays[-1])
        df.to_excel(f'''{root}/7日放量吸筹.xlsx''',index=False)

        # newDF = pd.DataFrame(df, columns = ["日期","股票代码","股票名称","战法名称","买入日期","买入价格"])
        # sqls = DataFrameToSqls_REPLACE(newDF,"zhanfa")
        # sql = " ".join(sqls)
        # self.dbConnection.Execute(sql)

        # sql1 = f'''SELECT `日期`,`股票代码`,`股票名称` FROM stock.zhanfa where `日期` = "{today}" and   `战法名称` = "N形战法" and `股票代码` not in (SELECT `股票代码` FROM stock.zhanfa where `日期` = "{yesterday}")'''
        # result1,columns1 = self.dbConnection.Query(sql1)
        # newDf1=pd.DataFrame(result1,columns=columns1)

        #DataFrameToJPG(newDf1,("股票代码","股票名称"),root,"N形战法_增量")
        DataFrameToJPG(df,("股票代码","股票名称"),root,"7日放量吸筹")
        print(df)

