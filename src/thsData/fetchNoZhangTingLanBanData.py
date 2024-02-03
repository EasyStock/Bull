
import pandas as pd
from decimal import Decimal
import re
from Utility.convertDataFrameToJPG import DataFrameToJPG
from workspace import workSpaceRoot,GetStockFolder

import os


class CFetchNoZhangTingData(object):
    #获取没有涨停，但是有长上影线的个股
    def __init__(self,dbConnection,date) -> None:
        self.dbConnection = dbConnection
        self.date = date
        self.dataFrame = None
    

    def QueryDataFromDB(self):
        sql = f'''SELECT A.*,B.`股票简称` FROM `stockdailyinfo` As A,`stockbasicinfo` As B where A.`日期` = "{self.date}" and A.`股票代码` = B.`股票代码`;'''
        results, columns = self.dbConnection.Query(sql)
        self.dataFrame = pd.DataFrame(results,columns=columns)

    def _ZuoRiShouPanJia(self,close,zhangfu):
        try:
            res = float(close)/(1+float(zhangfu)/100)
            return Decimal(res).quantize(Decimal("0.00"))
        except :
            return "-"
        
    
    def _IsHighEnough(self,stockID,close,high,closeY):
        zhangDiefu = (float(high) - float(closeY))/float(closeY)
        zhangDiefu2 = (float(high) - float(close))/float(closeY)
        if re.match('^00.*|^60.*',stockID) is not None:
            if zhangDiefu >= 0.07:
                if zhangDiefu2 >= 0.03:
                    return "yes_yes"
                return "yes"
            return "no"
        elif re.match('^30.*|^68.*',stockID) is not None:
            if zhangDiefu >= 0.13:
                if zhangDiefu2 >= 0.04:
                    return "yes_yes"
                return "yes"
            return "no"
        elif re.match('.*\BJ$',stockID) is not None:
            return "ignored"
        else:
            return "unKnown"
        

    def FetchNoZhangTingData(self):
        if self.dataFrame is None:
            self.QueryDataFromDB()

        
        df = self.dataFrame.copy()
        df['昨日收盘价'] = df.apply(lambda row: self._ZuoRiShouPanJia(row['收盘价'],row['涨跌幅']), axis=1)
        df['足够高'] = df.apply(lambda row: self._IsHighEnough(row['股票代码'],row['收盘价'],row['最高价'],row['昨日收盘价']), axis=1)
        
        print(df[df["足够高"] == "ignored"])
        print(df[df["足够高"] == "unKnown"])

        fodler = GetStockFolder(self.date)
        if os.path.exists(fodler) == False:
            os.makedirs(fodler)
        
        fileName1 = f'''长上影_{self.date}'''
        fileName2 = f'''涨停或接近涨停_{self.date}'''
        changShangYin = df[df["足够高"] == "yes_yes"].reset_index()
        jiejinZhangTing = df[df["足够高"] == "yes"].reset_index()

        DataFrameToJPG(changShangYin,["股票代码","股票简称"],fodler,fileName1)
        DataFrameToJPG(jiejinZhangTing,["股票代码","股票简称"],fodler,fileName2)
