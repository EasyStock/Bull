from Stock.KLine import CKLine
from Stock.StockInfo import CStockInfo
import pandas as pd
import re

COLUMNS_MAP= {
    'stockID' : '^股票代码|^指数代码|^转债代码',
    'stockName' :'^股票简称|^指数名称',
    'date' : "^日期",
    'volumn' : '^成交量',
    'open' : '^开盘价',
    'close' : '^收盘价',
    'high' : '^最高价',
    'low' : '^最低价',
}

class CKLineMgr(object):
    def __init__(self):
        pass

    @staticmethod
    def _formatVolumn(volumn:str):
        newVolumn = float(volumn[:-1])
        danwei = volumn[-1]
        ret = volumn
        if danwei == "万":
            ret = newVolumn*10000
        if danwei == "亿":
            ret = newVolumn*100000000
        return ret
    

    @staticmethod
    def PreprocessData(df: pd.DataFrame) -> pd.DataFrame:
        newDf = pd.DataFrame()
        columnsKeys = COLUMNS_MAP.keys()
        dfKeys = df.columns
        for key in columnsKeys:
            value = COLUMNS_MAP[key]
            for dfKey in dfKeys:
                if re.match(value, dfKey) != None:
                    newDf[key] = df[dfKey]

        newDf["volumn"] = newDf.apply(lambda row:CKLineMgr._formatVolumn(row["volumn"]),axis=1)
        newDf["volumn"] = newDf["volumn"].astype(float)
        newDf["open"] = newDf["open"].astype(float)
        newDf["close"] = newDf["close"].astype(float)
        newDf["high"] = newDf["high"].astype(float)
        newDf["low"] = newDf["low"].astype(float)
        newDf["yestoday_close"] = newDf["close"].shift()
        newDf["zhangfu"] = (newDf["close"] - newDf["yestoday_close"])/newDf["yestoday_close"] * 100
        return newDf


    @staticmethod
    def ReadFromDB(dbConnection, sql) -> list[CStockInfo]:
        result,columns = dbConnection.Query(sql)
        df =pd.DataFrame(result,columns=columns)

        newDf = CKLineMgr.PreprocessData(df)

        result = []
        for _,row in newDf.iterrows():
            kline = CStockInfo.create_from_series(row)
            result.append(kline)

        return result
    
    def ReadFromDataFrame(self, df: pd.DataFrame) -> list[CStockInfo]:
        newDf = CKLineMgr.PreprocessData(df)
        result = []
        for _,row in newDf.iterrows():
            kline = CStockInfo.create_from_series(row)
            result.append(kline)

        return result