import datetime
import pytz
import pandas as pd
from Utility.convertDataFrameToJPG import DataFrameToJPG
from workspace import GetStockFolder

class CBOLLPattern(object):
    '''
    BOLL 轨道收口战法
    Boll 轨道收口，上轨向下，下轨向上，上下轨距离在特定范围之内
    这应该是一个中枢震荡，
    寻找一个类2买位置， 由于BOLL 轨道收口，那么面临变盘，选择向上变盘还是向下变盘要看后续走势
    买点: 找到一个类2买位置，之前成交量要在5日，10日20日均量以下，最好有极度缩量过程
          
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
    

    def is_downward_trend_diff(self,series, threshold=0.6):
        """差分分析法：下降点比例超过阈值"""
        if len(series) < 2:
            return False
        
        decrease_count = 0

        for i in range(1, len(series)):
            if series.iloc[i] < series.iloc[i-1]:
                decrease_count += 1

        decrease_ratio = decrease_count / (len(series) - 1)
        return decrease_ratio > threshold
    
    def is_upward_diff(self,series, threshold=0.6):
        """
        差分分析法：上升点比例超过阈值
        参数:
            series: pandas Series
            threshold: 上升点比例阈值 (0-1)
        返回:
            bool: True(向上) 或 False(不向上)
        """
        if len(series) < 2:
            return False
        
        increase_count = 0
        for i in range(1, len(series)):
            if series.iloc[i] > series.iloc[i-1]:
                increase_count += 1
        
        increase_ratio = increase_count / (len(series) - 1)
        return increase_ratio >= threshold



    def _isBOLLPattern(self,df:pd.DataFrame)->bool:
        up = df['上轨'][-15:]
        down = df['下轨'][-15:]

        if self.is_downward_trend_diff(up,0.8) == False:
            return False
        

        if self.is_upward_diff(down,0.8) == False:
            return False
    
        subDF = df[-15:]
        condition = (subDF['MA20_成交量'] > subDF['MA10_成交量']) & (subDF['MA10_成交量'] > subDF['MA5_成交量']) & (0.8*subDF['MA5_成交量'] > subDF['成交量'])
        print(subDF[condition])

        return condition.any()


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
        newDf1["成交量"] = newDf1["成交量"].astype("float").round(3)


        groups = newDf1.groupby(["股票代码","股票简称"])
        res = []
        for (stockID,stockName), group in groups:
            df = group.reset_index()
            df.dropna()

            if df.shape[0]< 60:
                continue


            df["MA5"] = df["收盘价"].rolling(window=5).mean()
            df["MA10"] = df["收盘价"].rolling(window=10).mean()
            df["MA20"] = df["收盘价"].rolling(window=20).mean()

            df["MA5_成交量"] = df["成交量"].rolling(window=5).mean()
            df["MA10_成交量"] = df["成交量"].rolling(window=10).mean()
            df["MA20_成交量"] = df["成交量"].rolling(window=20).mean()

            df['中轨'] = df['收盘价'].rolling(window=20).mean()
            df['标准差'] = df['收盘价'].rolling(window=20).std(ddof=0)
            df['上轨'] = df['中轨'] + 2 * df['标准差']
            df['下轨'] = df['中轨'] - 2 * df['标准差']

            if self._isBOLLPattern(df) == False:
                continue

            data = {}
            data["股票代码"] = stockID
            data["股票简称"] = stockName
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
        df.reset_index(drop=True,inplace=True)
        root = GetStockFolder(tradingDays[-1])
        df.to_excel(f'''{root}/BOLL收口类二买战法.xlsx''',index=False)
        DataFrameToJPG(df,("股票代码","股票简称"),f'''{root}''',f'''BOLL收口类二买战法''')
        print(df)


