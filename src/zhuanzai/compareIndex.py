import pandas as pd
import re
from Utility.convertDataFrameToJPG import DataFrameToJPG
from workspace import workSpaceRoot
import os
import datetime

class CCompareWithIndex(object):
    def __init__(self,dbConnection,logger) -> None:
        self.dbConnection = dbConnection
        self.logger = logger

    def GetIndexInfo(self,date):
        sql = f'''SELECT * FROM stock.kaipanla_index where date = "{date}";'''
        result1,columns1 = self.dbConnection.Query(sql)
        newDf1=pd.DataFrame(result1,columns=columns1)
        newDf1.set_index(["StockID",],drop=True,inplace=True)
        return  newDf1

    def GetZhuanZhuaiInfo(self,today,yestoday):
        sql = f'''SELECT A.`日期`, A.`转债代码`,A.`转债名称`,A.`正股代码`, A.`正股名称`, A.`现价`,B.`现价` As '昨日价格' FROM stock.kezhuanzhai_all As A, (SELECT * FROM stock.kezhuanzhai_all where `日期`= "{yestoday}") AS B where A.`日期`= "{today}" and A.`转债代码` = B.`转债代码`;'''
        result1,columns1 = self.dbConnection.Query(sql)
        newDf1=pd.DataFrame(result1,columns=columns1)
        #print(newDf1)
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
    
    def ProcessData(self,indexInfo,df):
        for _, row in df.iterrows():
            stockID = row["正股代码"]
            kezhuanzaiID = row["转债代码"]
            price = row["现价"]
            price_last = row["昨日价格"]
            date =  row["日期"]
            if price_last == "None":
                price_last = 100

            zhangDiefu = (float(price) - float(price_last))/float(price_last)*100.0
            (IndexID,increase_rate,name) = self.GetIndexInfoBySotckID(indexInfo,stockID)
            if IndexID is None or increase_rate is None or name is None:
                continue
            delta = zhangDiefu - increase_rate
            #sql = f'''REPLACE INTO `stock`.`stockcompareindex` (`date`, `indexID`, `stockID`, `increase_rate`, `zhangdiefu`, `delta`) VALUES ('{date}', '{IndexID}', '{kezhuanzaiID}', '{increase_rate:.2f}', '{zhangDiefu:.2f}', '{delta:.2f}');'''
            sql = f'''REPLACE INTO `stock`.`stockcompareindex` (`date`, `indexID`, `stockID`, `increase_rate`, `zhangdiefu`, `delta`) VALUES ('{date}', '{IndexID}', '{kezhuanzaiID}', '{increase_rate:.2f}', '{zhangDiefu:.2f}', '{delta:.2f}');'''
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
            delta 
        0.1  -1.22 
        0.2  -0.72  
        0.3  -0.41  
        0.4  -0.15  
        0.5  0.08  
        0.6  0.30   
        0.7  0.54   
        0.8  0.81  
        0.9  1.27 
        '''
        sql = f'''SELECT * FROM stock.stockcompareindex;'''
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
            sql = f'''UPDATE `stock`.`stockcompareindex` SET `flag` = '{score}' WHERE (`date` = '{date}') and (`indexID` = '{indexID}') and (`stockID` = '{stockID}');'''
            #print(sql)
            self.dbConnection.Execute(sql)
            


    def CompareWithIndex(self,today,yestoday):
        self.logger.info(f'==============start to compare {today} ==============================')
        indexInfo = self.GetIndexInfo(today)
        zhuanZhaiInfo = self.GetZhuanZhuaiInfo(today,yestoday)
        self.ProcessData(indexInfo,zhuanZhaiInfo)
        self.logger.info(f'==============end of  {today} ==============================')

    def CompareWithIndex_ALL(self,tradingDays):
        size = len(tradingDays)
        for index,tradingDay, in enumerate(tradingDays):
            if index >= size -1:
                continue
            today = tradingDays[index+1]
            yesteday = tradingDay
            self.CompareWithIndex(today,yesteday)


class CZhuanzaiSelect(object):
    def __init__(self,dbConnection,logger) -> None:
        self.dbConnection = dbConnection
        self.logger = logger
        self.indexInfo = None
        self.zhuanZaiInfo = None

    def GetIndexInfo(self,start,end):
        sql = f'''SELECT * FROM stock.kaipanla_index where `date` >= "{start}" and `date` <= "{end}";'''
        result1,columns1 = self.dbConnection.Query(sql)
        self.indexInfo=pd.DataFrame(result1,columns=columns1)
        self.indexInfo.set_index(["StockID",],drop=True,inplace=True)

    def GetZhuanZhuaiInfo(self,today):
        sql = f'''SELECT * FROM stock.kezhuanzhai_all where `日期` = "{today}";'''
        result1,columns1 = self.dbConnection.Query(sql)
        self.zhuanZaiInfo=pd.DataFrame(result1,columns=columns1)
    

    def _formatInfo1(self,stockID,flagSum,group):
        indexID = list(group["indexID"])[0]
        info = self.zhuanZaiInfo[self.zhuanZaiInfo['转债代码'] == stockID]
        if info.empty:
            return ("",(stockID,"未知"))
        stockName = info.iloc[-1]["转债名称"]
        message =  f'''\n===================[{stockName}({stockID})]=========================
统计和:                     {flagSum}
'''
        return (message,(stockID,stockName))

    def _formatInfo2(self,stockID,flagSum,group):
        indexID = list(group["indexID"])[0]
        info = self.zhuanZaiInfo[self.zhuanZaiInfo['转债代码'] == stockID]
        indexInfo = self.indexInfo.loc[indexID]
        stockName = info.iloc[-1]["转债名称"]
        message =  f'''\n===================[{stockName}({stockID})]=========================
统计和:                     {flagSum}
------------------------------\n\n
转债信息:                   \n------------------------------\n{info}\n\n
指数信息:                   \n------------------------------\n{indexInfo}\n\n
指数比较详情:                \n------------------------------\n{str(group)}\n\n
'''
        return message
    

    def SelectFrom(self,start,end):
        sql = f'''SELECT * FROM stock.stockcompareindex where `date` >= "{start}" and `date` <= "{end}";'''
        result1,columns1 = self.dbConnection.Query(sql)
        df=pd.DataFrame(result1,columns=columns1)
        groups = df.groupby("stockID")
        res = []
        for stockID, group in groups:
            flagSum = group["flag"].sum()
            res.append((stockID,flagSum,group))

        res.sort(key=lambda x:x[1],reverse=True)
        return res

    def Select(self,start,end,tradingDays):
        pd.set_option('display.unicode.ambiguous_as_wide',True)
        pd.set_option('display.unicode.east_asian_width',True)
        pd.set_option('display.width',360)
        
        lastDay = tradingDays[-1]
        self.GetIndexInfo(start,end)
        self.GetZhuanZhuaiInfo(start)
        results = self.SelectFrom(start,end)
        outPut = []
        for result in results:
            (stockID,flagSum,group) = result
            if flagSum > 0:
                message = self._formatInfo1(stockID,flagSum,group)
                self.logger.info(message[0])
                outPut.append(message[1])
        jpgDataFrame = pd.DataFrame(outPut,columns=["转债代码","转债名称"])
        today = str(datetime.date.today())
        folderRoot= f'''{workSpaceRoot}/复盘/可转债/{today}/'''
        if os.path.exists(folderRoot) == False:
            os.makedirs(folderRoot)
       
        DataFrameToJPG(jpgDataFrame,["转债代码","转债名称"],folderRoot,f"{today}_比大盘")
        for result in results:
            (stockID,flagSum,group) = result
            if flagSum > 0:
                message = self._formatInfo2(stockID,flagSum,group)
                self.logger.info(message)
                input()
        

