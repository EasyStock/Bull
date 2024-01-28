import pandas as pd


class CVMASelecter(object):
    def __init__(self,dbConnection):
        self.dbConnection = dbConnection

    def _Select(self,stockID,zhangDiefu,VMA,VMAValue):
        sql = f'''SELECT * from  `stock`.`stockdailyinfo_traning_result` where `stockID` = "{stockID}" and VMA = "{VMA}" and `VMA值` <= "{VMAValue}";'''
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)
        df["涨幅"] = df['涨幅'].astype(float)
        df = df[df['涨幅']>=zhangDiefu]
        if df.empty:
            return (False,)
        print(df)
    
    def Select(self,date):
        sql = f'''SELECT * from  `stock`.`stockdailyinfo` where `日期` = "{date}";'''
        results, columns = self.dbConnection.Query(sql)
        self.df = pd.DataFrame(results,columns=columns)
        self.df['涨跌幅'] = self.df['涨跌幅'].astype(float)
        self.df['V/MA60'] = self.df['V/MA60'].astype(float)
        for _,row in self.df.iterrows():
            stockID = row["股票代码"]
            zhangDiefu = row["涨跌幅"]
            vma60 = row["V/MA60"]
            self._Select(stockID,zhangDiefu,"V/MA60",vma60)

