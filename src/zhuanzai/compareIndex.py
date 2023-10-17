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
        sql = f'''SELECT A.`日期`, A.`转债代码`,A.`转债名称`,A.`正股代码`, A.`正股名称`, A.`现价`,B.`现价` As '昨日价格' FROM stock.kezhuanzhai As A, (SELECT * FROM stock.kezhuanzhai where `日期`= "{yestoday}") AS B where A.`日期`= "{today}" and A.`转债代码` = B.`转债代码`;'''
        result1,columns1 = self.dbConnection.Query(sql)
        newDf1=pd.DataFrame(result1,columns=columns1)
        #print(newDf1)
        return newDf1

    def GetIndexInfoBySotckID(self,IndexInfo,stockID):
        if IndexInfo is None:
            return (None,None)
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
        
        increase_rate = IndexInfo.loc[IndexID]["increase_rate"]
        increase_rate = float(increase_rate.strip('%'))
        name = IndexInfo.loc[IndexID]["prod_name"]
        return (IndexID,increase_rate,name)
    
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
            delta = zhangDiefu - increase_rate
            #print(stockID,IndexID,name,float(increase_rate),zhangDiefu)
            flag = 0
            # delta
            # 0.02 -2.110
            # 0.05 -1.510
            # 0.10 -1.070
            # 0.50  0.020
            # 0.90  1.210
            # 0.95  1.610
            # 0.98  2.138
            if delta <-2.11:
                flag = -4
            elif delta <-1.5:
                flag = -3
            elif delta <-1:
                flag = -2
            elif delta <-0.5:
                flag = -1
            elif delta <0.5:
                flag = 0
            elif delta <1.2:
                flag = 1
            elif delta <1.6:
                flag = 2
            elif delta <2.1:
                flag = 3
            else :
                flag = 4
            #sql = f'''REPLACE INTO `stock`.`stockcompareindex` (`date`, `indexID`, `stockID`, `increase_rate`, `zhangdiefu`, `delta`) VALUES ('{date}', '{IndexID}', '{kezhuanzaiID}', '{increase_rate:.2f}', '{zhangDiefu:.2f}', '{delta:.2f}');'''
            sql = f'''REPLACE INTO `stock`.`stockcompareindex` (`date`, `indexID`, `stockID`, `increase_rate`, `zhangdiefu`, `delta`, `flag`) VALUES ('{date}', '{IndexID}', '{kezhuanzaiID}', '{increase_rate:.2f}', '{zhangDiefu:.2f}', '{delta:.2f}', '{flag}');'''
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
        sql = f'''SELECT * FROM stock.kezhuanzhai where `日期` = "{today}";'''
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
        

