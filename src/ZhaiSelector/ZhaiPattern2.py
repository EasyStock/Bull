from ZhaiSelector.ZhaiPatternBase import CZhaiPatternBase
import pandas as pd



class CZhaiPattern2(CZhaiPatternBase):
    def __init__(self,dbConnection):
        self.dbConnection = dbConnection

    def PatternName(self) -> str:
        return "140以下,单日涨幅大于5%"
    
    def _preProcess(self,df):
        df["最高价"] = df["最高价"].astype("float")
        df["最低价"] = df["最低价"].astype("float") 
        df["开盘价"] = df["开盘价"].astype("float")
        df["收盘价"] = df["收盘价"].astype("float")
        df["成交量"] = df["成交量"].astype("float")

        df["昨日收盘价"] = df["收盘价"].shift()
        df["今日开盘价涨幅"] = (df["开盘价"] - df["昨日收盘价"]) / df["昨日收盘价"] * 100
        df["今日收盘价涨幅"] = (df["收盘价"] - df["昨日收盘价"]) / df["昨日收盘价"] * 100
        return df

    def GetData(self,params:dict = {"startDay":"2024-01-01","lastDay":"2024-06-01"}):
        sql = f'''SELECT * FROM stock.kezhuanzai_ths where `日期` >= "{params["startDay"]}"'''
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)
        return df


    def SelectLast(self,params:dict = {"startDay":"2024-01-01","lastDay":"2024-06-01"}):
        df = self.GetData(params)
        groups = df.groupby("转债代码")
        all = []
        for _, group in groups:
            res = self._filterOne(group,params)
            if res is not None:
                all.append(res)

        df = pd.concat(all)
        df["筛选名称"] = self.PatternName()
        df.sort_values("日期",ascending=False,inplace=True)
        lastDay = params["lastDay"]
        return df[df["日期"] == lastDay]


    def _filterOne(self,df,params:dict):
        df = self._preProcess(df)
        newDf = df[df["今日收盘价涨幅"] > 8]
        newDf = newDf[newDf["昨日收盘价"] <140]
        if not newDf.empty:
            return newDf
        
        return None
    
    def SelectAll(self,params:dict):
        df = self.GetData(params)
        groups = df.groupby("转债代码")
        all = []
        for _, group in groups:
            res = self._filterOne(group,params)
            if res is not None:
                all.append(res)

        df = pd.concat(all)
        df["筛选名称"] = self.PatternName()
        df.sort_values("日期",ascending=False,inplace=True)
        return df
    

    def Descriptions(self,params:dict):
        pass

