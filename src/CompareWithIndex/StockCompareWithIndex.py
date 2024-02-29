import pandas as pd
import re
import numpy as np

class CStockCompareWithIndex(object):
    def __init__(self,dbConnection,logger) -> None:
        self.dbConnection = dbConnection
        self.logger = logger

    def GetIndexInfo(self,date):
        sql = f'''SELECT * FROM stock.kaipanla_index where date = "{date}";'''
        result1,columns1 = self.dbConnection.Query(sql)
        newDf1=pd.DataFrame(result1,columns=columns1)
        newDf1.set_index(["StockID",],drop=True,inplace=True)
        return  newDf1

    def GetStockInfo(self, date):
        sql1 = f'''
        SELECT * FROM stock.stockdailyinfo_2023 where `日期` = "{date}"
        UNION
        SELECT * FROM stock.stockdailyinfo where `日期` = "{date}"
        '''
        result1,columns1 = self.dbConnection.Query(sql1)
        newDf1=pd.DataFrame(result1,columns=columns1)
        return newDf1
    

    def GetIndexInfoBySotckID(self,IndexInfo,stockID):
        if IndexInfo is None:
            return (None,None,None)
        indexID = None
        ratio = 1
        if re.match('^00.*',stockID) is not None:
            IndexID = "SZ399001" #深证成指
        elif re.match('^30.*',stockID) is not None:
            IndexID = "SZ399006" #创业板指
            ratio = 0.5  # 创业板指 20%涨跌幅, 涨幅 %2
        elif re.match('^60.*',stockID) is not None:
            IndexID = "SH000001" #上证指数
        elif re.match('^68.*',stockID) is not None:
            IndexID = "SH000688" #科创50 
            ratio = 0.5  # 科创板20% 涨跌幅, 涨幅 %2
        else:
            IndexID =  None
        try:
            increase_rate = IndexInfo.loc[IndexID]["increase_rate"]
            increase_rate = float(increase_rate.strip('%'))
            name = IndexInfo.loc[IndexID]["prod_name"]
            return (IndexID,increase_rate,name,ratio)
        except:
            return (None,None,None,ratio)
    
    def ProcessData(self,indexInfo,df,date):
        for _, row in df.iterrows():
            stockID = row["股票代码"]
            zhangDiefu = float(row["涨跌幅"])
            (IndexID,increase_rate,name,ratio) = self.GetIndexInfoBySotckID(indexInfo,stockID)
            if IndexID is None or increase_rate is None or name is None:
                continue
            delta = (zhangDiefu - increase_rate) * ratio
            #print(f'''{name}  {zhangDiefu - increase_rate} {delta}''')
            sql = f'''REPLACE INTO `stock`.`compareindex_stock` (`date`, `indexID`, `stockID`, `increase_rate`, `zhangdiefu`, `delta`) VALUES ('{date}', '{IndexID}', '{stockID}', '{increase_rate:.2f}', '{zhangDiefu:.2f}', '{delta:.2f}');'''
            self.dbConnection.Execute(sql)
        
    def _score(self,volumn,percentail,reversed = False):
        result = 1
        for index, value in percentail.items():
            if float(volumn) <= float(value):
                result = float(index)
                break
        
        if reversed:
            result = 1.01 - float(result)
        
        result = int(result * 100)
        return result

    def score(self,df,column,newColumn,percentail:list,reversed=False):
        df[newColumn] = df.apply(lambda row: self._score(row[column],percentail[column],reversed), axis=1)

    def CalcScore(self,tradingDay):
        '''
        '''
        sql = f'''SELECT * FROM stock.compareindex_stock where `date` = "{tradingDay}";'''
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)
        df.set_index(["date","indexID","stockID"],drop=True,inplace=True)
        df["increase_rate"] = df["increase_rate"].astype(float)
        df["zhangdiefu"] = df["zhangdiefu"].astype(float)
        step = list(np.linspace(0, 1, 101))
        t = df.quantile(step)
        # print(t)
        newDf = df[pd.isna(df['flag'])]
        newDf.drop('flag', axis='columns',inplace=True)
        self.score(newDf,'delta','flag',t,False)
        for index, row in newDf.iterrows():
            score = row['flag']
            date = index[0]
            indexID = index[1]
            stockID = index[2]
            sql = f'''UPDATE `stock`.`compareindex_stock` SET `flag` = '{score}' WHERE (`date` = '{date}') and (`indexID` = '{indexID}') and (`stockID` = '{stockID}');'''
            #print(sql)
            self.dbConnection.Execute(sql)
        

    def CompareWithIndex(self,today):
        self.logger.info(f'==============start to compare {today} ==============================')
        indexInfo = self.GetIndexInfo(today)
        stockInfo = self.GetStockInfo(today)
        self.ProcessData(indexInfo,stockInfo,today)
        self.CalcScore(today)
        self.logger.info(f'==============end of  {today} ==============================')

    def CompareWithIndex_ALL(self,tradingDays):
        for _,tradingDay, in enumerate(tradingDays):
            self.CompareWithIndex(tradingDay)
            
