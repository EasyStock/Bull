import pandas as pd
import re

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
        if re.match('^00.*',stockID) is not None:
            IndexID = "SZ399001" #深证成指
        elif re.match('^30.*',stockID) is not None:
            IndexID = "SZ399006" #创业板指
        elif re.match('^60.*',stockID) is not None:
            IndexID = "SH000001" #上证指数
        elif re.match('^68.*',stockID) is not None:
            IndexID = "SH000688" #科创50 
        else:
            IndexID =  None
        try:
            increase_rate = IndexInfo.loc[IndexID]["increase_rate"]
            increase_rate = float(increase_rate.strip('%'))
            name = IndexInfo.loc[IndexID]["prod_name"]
            return (IndexID,increase_rate,name)
        except:
            return (None,None,None)
    
    def ProcessData(self,indexInfo,df,date):
        for _, row in df.iterrows():
            stockID = row["股票代码"]
            zhangDiefu = float(row["涨跌幅"])
            (IndexID,increase_rate,name) = self.GetIndexInfoBySotckID(indexInfo,stockID)
            if IndexID is None or increase_rate is None or name is None:
                continue
            delta = zhangDiefu - increase_rate
            sql = f'''REPLACE INTO `stock`.`compareindex_stock` (`date`, `indexID`, `stockID`, `increase_rate`, `zhangdiefu`, `delta`) VALUES ('{date}', '{IndexID}', '{stockID}', '{increase_rate:.2f}', '{zhangDiefu:.2f}', '{delta:.2f}');'''
            self.dbConnection.Execute(sql)
        
    def _score(self,volumn,percentail,reversed = False):
        result = 1
        for index, value in percentail.items():
            if float(volumn) <= float(value):
                result = float(index)
                break
        
        if reversed:
            result = 1.1 - float(result)
        
        result = int(result * 10)
        return result

    def score(self,df,column,newColumn,percentail:list,reversed=False):
        df[newColumn] = df.apply(lambda row: self._score(row[column],percentail[column],reversed), axis=1)

    def CalcScore(self):
        '''
        '''
        sql = f'''SELECT * FROM stock.compareindex_stock;'''
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)
        df.set_index(["date","indexID","stockID"],drop=True,inplace=True)
        df["increase_rate"] = df["increase_rate"].astype(float)
        df["zhangdiefu"] = df["zhangdiefu"].astype(float)
        t = df.quantile([0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9])
        print(t)
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
        self.logger.info(f'==============end of  {today} ==============================')

    def CompareWithIndex_ALL(self,tradingDays):
        for _,tradingDay, in enumerate(tradingDays):
            self.CompareWithIndex(tradingDay)
        self.CalcScore()
