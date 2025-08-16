import datetime
import pytz
import pandas as pd
from Utility.convertDataFrameToJPG import DataFrameToJPG
from workspace import GetStockFolder

class CLowerHigherPattern(object):
    '''
    新低战法:股价创新低,如果创新低，可以去筛选出有没有两中枢背驰，去做一买
    新高战法:股价创新高,如果创新高，可以去筛选出有没有两中枢背驰，去做一卖
    '''
    def __init__(self,dbConnection,lastNDays = 500):
        self.dbConnection = dbConnection
        self.lastNDays = lastNDays
        self.tradingDays = []
        self.columns = []
    
    def GetTradingDates(self):
        today = datetime.datetime.now(pytz.timezone('Asia/Shanghai')).date()
        end = today.strftime("%Y-%m-%d")
        sql = f"SELECT `日期` FROM stock.treadingDay where `开市` =1 and `交易所`='SSE' and `日期`<='{end}' order by `日期` DESC limit {self.lastNDays};"
        res,_ = self.dbConnection.Query(sql)
        self.tradingDays = [r[0] for r in reversed(res)]
        return self.tradingDays

    def _isHighestOrLowest(self,df:pd.DataFrame,highestPrice,lowestPrice):
            if df.shape[0] < 10:
                return (False,None,None)
            
            for index in range(1,6):
                row = df.iloc[-index]

                if row["收盘价"] == lowestPrice:
                    return (True,row,"新低战法")
                
                if  row["收盘价"] == highestPrice:
                    return (True,row,"新高战法")
            
            return (False,None,None)


    def GetStockData(self,startDay):
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
        for (stockID,stockName), group in groups:
            df = group.reset_index()
            df.dropna()


            lowestPrice = df["最低价"].min()
            highestPrice = df["最高价"].max()
            
            flag,row,name = self._isHighestOrLowest(df,highestPrice,lowestPrice)
            if flag == False:
                continue

            date1 = datetime.datetime.strptime(self.tradingDays[-1], "%Y-%m-%d")
            date2 = datetime.datetime.strptime(row["日期"], "%Y-%m-%d")
            
            # 计算日期差
            delta = date1 - date2
            if delta.days > 8: # 停牌超过5天
                continue

            data = {}
            data["股票代码"] = stockID
            data["股票简称"] = stockName

            data["最低价格"] = lowestPrice
            data["最高价格"] = lowestPrice
            data["开始时间"] = startDay
            data["发生时间"] = row["日期"]
            if name == "新低战法":
                data["发生价格"] = row["最低价"]
            elif name == "新高战法":
                data["发生价格"] = row["最高价"]
            data["战法名称"] = name
            res.append(data)

        return res
    
    def Select(self):
        tradingDays = self.GetTradingDates()
        res = self.GetStockData(tradingDays[-300])
        self.columns = ["股票代码","股票简称","最低价格","开始时间","发生时间","发生价格","战法名称"]
        df = pd.DataFrame(res,columns = self.columns)
        df = df[df['股票简称'].str.match('[\s\S]*(ST|退)+?[\s\S]*') == False]
        df = df[df['股票代码'].str.match('[\s\S]*(BJ|^688|^30)+?[\s\S]*') == False]
        df.sort_values("发生时间",ascending=False,inplace=True)

        groups = df.groupby(["战法名称"])
        root = GetStockFolder(tradingDays[-1])
        for name, group in groups:
            group.to_excel(f'''{root}/{name[0]}.xlsx''',index=False)
            DataFrameToJPG(group,("股票代码","股票简称"),f'''{root}''',f'''{name[0]}''')
            print(group)


