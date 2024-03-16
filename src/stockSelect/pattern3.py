import datetime
import pytz
import pandas as pd
from Utility.convertDataFrameToJPG import DataFrameToJPG


class CDiFengXing(object):
    @staticmethod
    def isDiFengXing(open:pd.Series,close:pd.Series,high:pd.Series,low:pd.Series):
        if open.count() <3 or close.count() <3 or low.count() <3 or high.count()<3:
            return False
        
        #中间K线是阴线，中间线的收盘价是最低价，中间线的开盘价也是最低价
        if close.iloc[-2] >= open.iloc[-2]:  #第二根是阴线
            return False 
        
        max1 = max(open.iloc[-1],close.iloc[-1])
        max3 = max(open.iloc[-3],close.iloc[-3])

        min1 = min(open.iloc[-1],close.iloc[-1])
        min3 = min(open.iloc[-3],close.iloc[-3])

        flag1 = close.iloc[-2] < min1 and close.iloc[-2] > min3 
        flag2 = open.iloc[-2] < max1 and open.iloc[-2] < max3

        if not flag1:
            return False
        
        if not flag2:
            return False
        
        return True

class CDingFengXing(object):
    @staticmethod
    def isDingFenXing(open:pd.Series,close:pd.Series,high:pd.Series,low:pd.Series):
        #中间K线是阳线，中间线的收盘价是最高价，中间线的开盘价也是最高价
        if open.count() <3 or close.count() <3 or low.count() <3 or high.count()<3:
            return False
        
        if close.iloc[-2] <= open.iloc[-2]:  #第二根是阳线
            return False 
        
        max1 = max(open.iloc[-1],close.iloc[-1])
        max3 = max(open.iloc[-3],close.iloc[-3])

        min1 = min(open.iloc[-1],close.iloc[-1])
        min3 = min(open.iloc[-3],close.iloc[-3])

        flag1 = close.iloc[-2] >max1 and close.iloc[-2] > max3
        flag2 = open.iloc[-2] > min1 and open.iloc[-2] >min3

        if not flag1:
            return False
        
        if not flag2:
            return False
                
        return True

class CDingFengXingTingDun(object):
    @staticmethod
    def isDingFengXingTingDun(open:pd.Series,close:pd.Series,high:pd.Series,low:pd.Series):
        if open.count() <4 or close.count() <4 or low.count() <4 or high.count()<4:
            return False
        
        open1 = open[:-1]
        close1 = close[:-1]
        high1 = high[:-1]
        low1 = low[:-1]
        if CDingFengXing.isDingFenXing(open1, close1, high1, low1) == False:
            return False
        
        min1 = min(low.iloc[-4],low.iloc[-3],low.iloc[-2])
        if close.iloc[-1] < min1:
            return True
        
        return False


class CDiFengXingTingDun(object):
    @staticmethod
    def isDiFengXingTingDun(open:pd.Series,close:pd.Series,high:pd.Series,low:pd.Series):
        if open.count() <4 or close.count() <4 or low.count() <4 or high.count()<4:
            return False
        
        open1 = open[:-1]
        close1 = close[:-1]
        high1 = high[:-1]
        low1 = low[:-1]
        if CDiFengXing.isDiFengXing(open1,close1,high1,low1) == False:
            return False

        max1 = max(high.iloc[-4],high.iloc[-3],high.iloc[-2])
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
        if CDiFengXingTingDun.isDiFengXingTingDun(open,close,high,low) == True:
            return "底分型停顿"
        
        if CDingFengXingTingDun.isDingFengXingTingDun(open,close,high,low) == True:
            return "顶分型停顿"
        
        if CDiFengXing.isDiFengXing(open, close, high, low) == True:
            return "底分型"
        
        if CDingFengXing.isDingFenXing(open, close, high, low) == True:
            return "顶分型"
        
        return None

    def GetStockData(self,startDay):
        sql1 = f'''
        SELECT B.`日期`,A.`股票代码`,A.`股票简称`,B.`开盘价`,B.`收盘价`,B.`最高价`,B.`最低价` FROM stock.stockbasicinfo AS A,(SELECT `日期`,`股票代码`,`开盘价`,`收盘价`,`最高价`,`最低价` FROM stock.stockdailyinfo where `日期` >= "{startDay}") AS B where A.`股票代码` = B.`股票代码`
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
    
    def Select(self):
        tradingDays = self.GetTradingDates()
        res = self.GetStockData(tradingDays[0])
        res1 = self.GetIndexData(tradingDays[0])
        data = res + res1
        df = pd.DataFrame(data,columns = ["股票代码","股票简称","结果"])
        df = df[df['股票简称'].str.match('[\s\S]*(ST|退)+?[\s\S]*') == False]
        df = df[df['股票代码'].str.match('[\s\S]*(BJ|^688|^30)+?[\s\S]*') == False]

        diFengXingTingDun = df[df["结果"] == "底分型停顿"]
        diFengXingTingDun.to_excel('/tmp/底分型停顿.xlsx',index=False)
        DataFrameToJPG(diFengXingTingDun,("股票代码","股票简称"),"/tmp/","底分型停顿")

        dingFengxingTingDun = df[df["结果"] == "顶分型停顿"]
        dingFengxingTingDun.to_excel('/tmp/顶分型停顿.xlsx',index=False)
        DataFrameToJPG(dingFengxingTingDun,("股票代码","股票简称"),"/tmp/","顶分型停顿")

        diFengXing = df[df["结果"] == "底分型"]
        diFengXing.to_excel('/tmp/底分型.xlsx',index=False)
        DataFrameToJPG(diFengXing,("股票代码","股票简称"),"/tmp/","底分型")

        DingFengXing = df[df["结果"] == "顶分型"]
        DingFengXing.to_excel('/tmp/顶分型.xlsx',index=False)
        DataFrameToJPG(DingFengXing,("股票代码","股票简称"),"/tmp/","顶分型")

        df.to_excel('/tmp/分型.xlsx',index=False)
        DataFrameToJPG(df,("股票代码","股票简称"),"/tmp/","分型_all")     


