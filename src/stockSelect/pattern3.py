import datetime
import pytz
import pandas as pd
from Utility.convertDataFrameToJPG import DataFrameToJPG
from workspace import GetStockFolder


class CDiFengXing(object):
    @staticmethod
    def isDiFengXing(open:pd.Series,close:pd.Series,high:pd.Series,low:pd.Series):
        if open.count() <3 or close.count() <3 or low.count() <3 or high.count()<3:
            return False
        
        # #中间K线是阴线，中间线的收盘价是最低价，中间线的开盘价也是最低价
        # if close.iloc[-2] >= open.iloc[-2]:  #第二根是阴线
        #     return False 
        
        max1 = max(open.iloc[-1],close.iloc[-1])
        max3 = max(open.iloc[-3],close.iloc[-3])

        min1 = min(open.iloc[-1],close.iloc[-1])
        min3 = min(open.iloc[-3],close.iloc[-3])

        low2 = min(open.iloc[-2],close.iloc[-2])
        high2 = max(open.iloc[-2],close.iloc[-2])

        flag1 = low2 < min1 and low2 < min3 
        flag2 = high2 < max1 and high2 < max3

        if not flag1:
            return False
        
        if not flag2:
            return False
        
        return True

class CDingFengXing(object):
    @staticmethod
    def isDingFenXing(open:pd.Series,close:pd.Series,high:pd.Series,low:pd.Series):
        if open.count() <3 or close.count() <3 or low.count() <3 or high.count()<3:
            return False
        
        # 中间K线是阳线，中间线的收盘价是最高价，中间线的开盘价也是最高价
        # if close.iloc[-2] <= open.iloc[-2]:  #第二根是阳线
        #     return False 

        max1 = max(open.iloc[-1],close.iloc[-1])
        max3 = max(open.iloc[-3],close.iloc[-3])

        min1 = min(open.iloc[-1],close.iloc[-1])
        min3 = min(open.iloc[-3],close.iloc[-3])

        low2 = min(open.iloc[-2],close.iloc[-2])
        high2 = max(open.iloc[-2],close.iloc[-2])

        flag1 = high2 > max1 and high2 > max3
        flag2 = low2 > min1 and low2 > min3

        if not flag1:
            return False
        
        if not flag2:
            return False
                
        return True

class CDingFengXingTingDun(object):
    @staticmethod
    def isDingFengXingTingDun(open:pd.Series,close:pd.Series,high:pd.Series,low:pd.Series,middle = 0):
        miniSize = 4 + middle
        if open.count() <miniSize or close.count() <miniSize or low.count() <miniSize or high.count()<miniSize:
            return False
        
        last =  middle + 1

        open1 = open[:-last]
        close1 = close[:-last]
        high1 = high[:-last]
        low1 = low[:-last]

        if CDingFengXing.isDingFenXing(open1, close1, high1, low1) == False:
            return False
        
        min1 = min(low.iloc[-middle-4],low.iloc[-middle-3],low.iloc[-middle-2])
        if middle >=1:
            for i in range(2,middle+2):
                if close.iloc[-i] < min1:
                    return False
                
        if close.iloc[-1] < min1:
            return True
        
        return False


class CDiFengXingTingDun(object):
    @staticmethod
    def isDiFengXingTingDun(open:pd.Series,close:pd.Series,high:pd.Series,low:pd.Series,middle = 0):
        miniSize = 4 + middle
        if open.count() <miniSize or close.count() <miniSize or low.count() <miniSize or high.count()<miniSize:
            return False
        
        last =  middle + 1
        open1 = open[:-last]
        close1 = close[:-last]
        high1 = high[:-last]
        low1 = low[:-last]

        if CDiFengXing.isDiFengXing(open1,close1,high1,low1) == False:
            return False

        max1 = max(high.iloc[-middle - 4],high.iloc[-middle - 3],high.iloc[-middle - 2])

        if middle >=1:
            for i in range(2,middle+2):
                if close.iloc[-i] > max1:
                    return False

        if close.iloc[-1] > max1:
            return True

        return False

class CStockPattern3(object):
    '''
    顶底分型
    '''
    def __init__(self,dbConnection,lastNDays = 20):
        self.dbConnection = dbConnection
        self.lastNDays = lastNDays
    
    def GetTradingDates(self):
        today = datetime.datetime.now(pytz.timezone('Asia/Shanghai')).date()
        end = today.strftime("%Y-%m-%d")
        sql = f"SELECT `日期` FROM stock.treadingDay where `开市` =1 and `交易所`='SSE' and `日期`<='{end}' order by `日期` DESC limit {self.lastNDays};"
        res,_ = self.dbConnection.Query(sql)
        self.tradingDays = [r[0] for r in reversed(res)]
        return self.tradingDays

    def _IsFengXing(self,open:pd.Series,close:pd.Series,high:pd.Series,low:pd.Series):
        for i in range(0,6):
            if CDiFengXingTingDun.isDiFengXingTingDun(open,close,high,low,i) == True:
                res = f'''底分型停顿_{i}'''
                return res
        
        for i in range(0,10):
            if CDingFengXingTingDun.isDingFengXingTingDun(open,close,high,low,i) == True:
                return f'''顶分型停顿_{i}'''
        
        if CDiFengXing.isDiFengXing(open, close, high, low) == True:
            return "底分型"
        
        if CDingFengXing.isDingFenXing(open, close, high, low) == True:
            return "顶分型"
        
        return None
    
    def _AnalysisToSQL(self,day,tableName,df,stockID,columnName):
        sql = None
        fengxing = self._IsFengXing(df['开盘价'],df['收盘价'],df['最高价'],df['最低价'])
        if fengxing is not None:
            sql = f'''UPDATE `{tableName}` SET `顶底分型` = '{fengxing}' WHERE (`日期` = '{day}') and (`{columnName}` = '{stockID}');'''

        return sql

    def _AnalysisWithDF(self,df,tableName,columnName):
        date = df["日期"].iloc[-1]
        stockID =  df["股票代码"].iloc[-1]
        return self._AnalysisToSQL(date,tableName,df,stockID,columnName)
    
    def AnalysisIndexEveryDay(self,startDay):
        sql1 = f'''
        SELECT `日期`,`指数代码` as `股票代码`,`指数名称` as `股票简称`,`开盘价(点)` as `开盘价`,`收盘价(点)` as `收盘价`,`最高价(点)` as `最高价` ,`最低价(点)` as `最低价` FROM stock.index_dailyinfo where `日期` >= "{startDay}";
        '''
        result1,columns1 = self.dbConnection.Query(sql1)
        newDf1=pd.DataFrame(result1,columns=columns1)
        groups = newDf1.groupby(["股票代码","股票简称"])
        sqls = []
        for stockID, group in groups:
            df = group.reset_index()
            df.dropna()
            df["开盘价"] = df["开盘价"].astype("float")
            df["收盘价"] = df["收盘价"].astype("float")
            df["最高价"] = df["最高价"].astype("float")
            df["最低价"] = df["最低价"].astype("float")
            sql = self._AnalysisWithDF(df,"index_dailyinfo","指数代码")
            if sql is not None:
                sqls.append(sql)

            for i in range(1,df.shape[0]):
                newDf = df[:-i]
                sql = self._AnalysisWithDF(newDf,"index_dailyinfo","指数代码")
                if sql is not None:
                    sqls.append(sql)

        for sql in sqls:
            self.dbConnection.Execute(sql)
    
    def AnalysisBanKuaiEveryDay(self,startDay):
        sql1 = f'''
        SELECT `日期`,`板块代码` as `股票代码`,`板块名称` as `股票简称`,`开盘价(点)` as `开盘价`,`收盘价(点)` as `收盘价`,`最高价(点)` as `最高价` ,`最低价(点)` as `最低价` FROM stock.bankuai_index_dailyinfo where `日期` >= "{startDay}";
        '''
        result1,columns1 = self.dbConnection.Query(sql1)
        newDf1=pd.DataFrame(result1,columns=columns1)
        groups = newDf1.groupby(["股票代码","股票简称"])
        sqls = []
        for stockID, group in groups:
            df = group.reset_index()
            df.dropna()
            df["开盘价"] = df["开盘价"].astype("float")
            df["收盘价"] = df["收盘价"].astype("float")
            df["最高价"] = df["最高价"].astype("float")
            df["最低价"] = df["最低价"].astype("float")
            sql = self._AnalysisWithDF(df,"bankuai_index_dailyinfo","板块代码")
            if sql is not None:
                sqls.append(sql)

            for i in range(1,df.shape[0]):
                newDf = df[:-i]
                sql = self._AnalysisWithDF(newDf,"bankuai_index_dailyinfo","板块代码")
                if sql is not None:
                    sqls.append(sql)

        for sql in sqls:
            self.dbConnection.Execute(sql)

    def AnalysisStockEveryDay(self,startDay):
        sql1 = f'''
        SELECT B.`日期`,A.`股票代码`,A.`股票简称`,B.`开盘价`,B.`收盘价`,B.`最高价`,B.`最低价` FROM stock.stockbasicinfo AS A,(SELECT `日期`,`股票代码`,`开盘价`,`收盘价`,`最高价`,`最低价` FROM stock.stockdailyinfo where `日期` >= "{startDay}") AS B where  A.`股票代码` = B.`股票代码`
        '''
        result1,columns1 = self.dbConnection.Query(sql1)
        newDf1=pd.DataFrame(result1,columns=columns1)
        groups = newDf1.groupby(["股票代码","股票简称"])
        sqls = []
        for stockID, group in groups:
            df = group.reset_index()
            df.dropna()
            df["开盘价"] = df["开盘价"].astype("float")
            df["收盘价"] = df["收盘价"].astype("float")
            df["最高价"] = df["最高价"].astype("float")
            df["最低价"] = df["最低价"].astype("float")
            sql = self._AnalysisWithDF(df,"stockdailyinfo","股票代码")
            if sql is not None:
                sqls.append(sql)

            for i in range(1,df.shape[0]):
                newDf = df[:-i]
                sql = self._AnalysisWithDF(newDf,"stockdailyinfo","股票代码")
                if sql is not None:
                    sqls.append(sql)

        for sql in sqls:
            self.dbConnection.Execute(sql)


    def GetStockData(self,startDay):
        sql1 = f'''
        SELECT B.`日期`,A.`股票代码`,A.`股票简称`,B.`开盘价`,B.`收盘价`,B.`最高价`,B.`最低价` FROM stock.stockbasicinfo AS A,(SELECT `日期`,`股票代码`,`开盘价`,`收盘价`,`最高价`,`最低价` FROM stock.stockdailyinfo where `日期` >= "{startDay}") AS B where  A.`股票代码` = B.`股票代码`
        '''
        result1,columns1 = self.dbConnection.Query(sql1)
        newDf1=pd.DataFrame(result1,columns=columns1)
        groups = newDf1.groupby(["股票代码","股票简称"])
        res = []
        for stockID, group in groups:
            df = group.reset_index()
            df.dropna()
            df["开盘价"] = df["开盘价"].astype("float")
            df["收盘价"] = df["收盘价"].astype("float")
            df["最高价"] = df["最高价"].astype("float")
            df["最低价"] = df["最低价"].astype("float")
            fengxing = self._IsFengXing(df['开盘价'],df['收盘价'],df['最高价'],df['最低价'])
            if  fengxing is not None:
                data = []
                data.extend(stockID)
                data.append(fengxing)
                res.append(data)

        return res
    
    def GetIndexData(self,startDay):
        sql1 = f'''
        SELECT `指数代码` as `股票代码`,`指数名称` as `股票简称`,`开盘价(点)` as `开盘价`,`收盘价(点)` as `收盘价`,`最高价(点)` as `最高价` ,`最低价(点)` as `最低价` FROM stock.index_dailyinfo where `日期` >= "{startDay}";
        '''
        result1,columns1 = self.dbConnection.Query(sql1)
        newDf1=pd.DataFrame(result1,columns=columns1)
        groups = newDf1.groupby(["股票代码","股票简称"])
        res = []
        for stockID, group in groups:
            df = group.reset_index()
            df.dropna()
            df["开盘价"] = df["开盘价"].astype("float")
            df["收盘价"] = df["收盘价"].astype("float")
            df["最高价"] = df["最高价"].astype("float")
            df["最低价"] = df["最低价"].astype("float")
            fengxing = self._IsFengXing(df['开盘价'],df['收盘价'],df['最高价'],df['最低价'])
            if  fengxing is not None:
                data = []
                data.extend(stockID)
                data.append(fengxing)
                res.append(data)

        return res

    def GetBanKuaiIndexData(self,startDay):
        sql1 = f'''
        SELECT `板块代码` as `股票代码`,`板块名称` as `股票简称`,`开盘价(点)` as `开盘价`,`收盘价(点)` as `收盘价`,`最高价(点)` as `最高价` ,`最低价(点)` as `最低价` FROM stock.bankuai_index_dailyinfo where `日期` >= "{startDay}";
        '''
        result1,columns1 = self.dbConnection.Query(sql1)
        newDf1=pd.DataFrame(result1,columns=columns1)
        groups = newDf1.groupby(["股票代码","股票简称"])
        res = []
        for stockID, group in groups:
            df = group.reset_index()
            df.dropna()
            df["开盘价"] = df["开盘价"].astype("float")
            df["收盘价"] = df["收盘价"].astype("float")
            df["最高价"] = df["最高价"].astype("float")
            df["最低价"] = df["最低价"].astype("float")
            fengxing = self._IsFengXing(df['开盘价'],df['收盘价'],df['最高价'],df['最低价'])
            if  fengxing is not None:
                data = []
                data.extend(stockID)
                data.append(fengxing)
                res.append(data)

        return res
    

    def AnalysisEveryDay(self,lastNDays):
        self.lastNDays = lastNDays
        tradingDays = self.GetTradingDates()
        # self.AnalysisIndexEveryDay(tradingDays[0])
        # self.AnalysisBanKuaiEveryDay(tradingDays[0])
        self.AnalysisStockEveryDay(tradingDays[0])

    def _AnalysisLastDay(self,res,date,type="指数"):
        data = res
        df = pd.DataFrame(data,columns = ["股票代码","股票简称","结果"])
        df = df[df['股票简称'].str.match('[\s\S]*(ST|退)+?[\s\S]*') == False]
        df = df[df['股票代码'].str.match('[\s\S]*(BJ|^688|^30)+?[\s\S]*') == False]
        if df.empty:
            return
        
        stockFolder = GetStockFolder(date)
        diFengXingTingDun = df[df["结果"].str.match("底分型停顿")]
        if not diFengXingTingDun.empty:
            diFengXingTingDun.to_excel(f'''{stockFolder}/{type}_底分型停顿.xlsx''',index=False)
            DataFrameToJPG(diFengXingTingDun,("股票代码","股票简称"),stockFolder,f'''{type}_底分型停顿''')


        dingFengxingTingDun = df[df["结果"].str.match("顶分型停顿")]
        if not dingFengxingTingDun.empty:
            dingFengxingTingDun.to_excel(f'''{stockFolder}/{type}_顶分型停顿.xlsx''',index=False)
            DataFrameToJPG(dingFengxingTingDun,("股票代码","股票简称"),stockFolder,f'''{type}_顶分型停顿''')

        diFengXing = df[df["结果"].str.match("底分型$")]
        if not diFengXing.empty:
            diFengXing.to_excel(f'''{stockFolder}/{type}_底分型.xlsx''',index=False)
            DataFrameToJPG(diFengXing,("股票代码","股票简称"),stockFolder,f'''{type}_底分型''')

        DingFengXing = df[df["结果"].str.match("顶分型$")]
        if not DingFengXing.empty:
            DingFengXing.to_excel(f'''{stockFolder}/顶分型.xlsx''',index=False)
            DataFrameToJPG(DingFengXing,("股票代码","股票简称"),stockFolder,f'''{type}_顶分型''')

        df.to_excel(f'''{stockFolder}/{type}_分型.xlsx''',index=False)
        #DataFrameToJPG(df,("股票代码","股票简称"),stockFolder,f'''{type}_分型_all''')  

    def AnalysisLastDay(self):
        self.lastNDays = 10
        tradingDays = self.GetTradingDates()
        fristDay = tradingDays[0]
        lastDay = tradingDays[-1]
        res = self.GetStockData(fristDay)
        res1 = self.GetIndexData(fristDay)
        res2 = self.GetBanKuaiIndexData(fristDay)
        self._AnalysisLastDay(res,lastDay,"股票")
        self._AnalysisLastDay(res1,lastDay,"指数")
        self._AnalysisLastDay(res2,lastDay,"板块股票")


    def _DaLiang(self,sql,ColumnVolumnName):
        result1,columns1 = self.dbConnection.Query(sql)
        newDf1=pd.DataFrame(result1,columns=columns1)
        groups = newDf1.groupby(["股票代码","股票简称"])
        results = []
        for stockID, group in groups:
            if group.shape[0] < 3:
                continue
            v1 =  float(group[ColumnVolumnName].iloc[-1].replace("万","").replace("亿",""))
            v2 =  float(group[ColumnVolumnName].iloc[-2].replace("万","").replace("亿",""))
            v3 =  float(group[ColumnVolumnName].iloc[-3].replace("万","").replace("亿",""))
            if v1 > v2 and v1 > v3:
                res = []
                res.extend(stockID)
                res.append(group["顶底分型"].iloc[-1])
                results.append(res)

        return results

    def GetBaoLiangDiFenXing(self):
        tradingDays = self.GetTradingDates()
        last3 = tradingDays[-3]
        last1 = tradingDays[-1]
        sql1 = f'''SELECT `指数代码` as `股票代码`,`指数名称` as `股票简称`,`成交额(元)`,`顶底分型` FROM stock.index_dailyinfo where  `指数代码` in (SELECT `指数代码` FROM stock.index_dailyinfo where `顶底分型` is not NULL and `日期` = "{last1}") and `日期` >= "{last3}";'''
        res1 = self._DaLiang(sql1,"成交额(元)")
        self._AnalysisLastDay(res1,last1,"底分型_大量_指数")

        sql2 = f'''SELECT `板块代码` as `股票代码`,`板块名称` as `股票简称`,`成交额(元)`,`顶底分型` FROM stock.bankuai_index_dailyinfo where  `板块代码` in (SELECT `板块代码` FROM stock.bankuai_index_dailyinfo where `顶底分型` is not NULL and `日期` = "{last1}") and `日期` >= "{last3}"'''
        res2 = self._DaLiang(sql2,"成交额(元)")
        self._AnalysisLastDay(res2,last1,"底分型_大量_板块")

        sql3 = f'''SELECT B.`日期`, A.`股票代码`, A.`股票简称`, B.`成交量`, B.`顶底分型` FROM stock.stockbasicinfo AS A,(SELECT `日期`,`股票代码`,`成交量`,`顶底分型` from stock.stockdailyinfo where `股票代码` in (SELECT `股票代码` FROM stock.stockdailyinfo where `顶底分型` is not NULL and `日期` = "{last1}") and `日期` >= "{last3}") AS B where  A.`股票代码` = B.`股票代码` ;'''
        res3 = self._DaLiang(sql3,"成交量")
        self._AnalysisLastDay(res3,last1,"底分型_大量_股票")


    def Select(self):
        self.AnalysisEveryDay(10)
        #self.AnalysisLastDay()
        self.GetBaoLiangDiFenXing()


