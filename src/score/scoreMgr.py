import pandas as pd
from Utility.convertDataFrameToJPG import DataFrameToJPG
from workspace import workSpaceRoot
import os

class CScoreMgr(object):
    def __init__(self,dbConnection) -> None:
        self.dbConnection = dbConnection
        self.ZhuanZaiInfo = None
        self.QuantileDf = None

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

    def GetKeZhuanZaiInfo(self,date):
        sql = f'''select * FROM stock.kezhuanzhai_all where `日期` = "{date}";'''
        results, columns = self.dbConnection.Query(sql)
        self.ZhuanZaiInfo = pd.DataFrame(results,columns=columns)
        self.ZhuanZaiInfo.set_index(["转债代码",],drop=True,inplace=True)
        self.ZhuanZaiInfo['剩余规模'] = self.ZhuanZaiInfo['剩余规模'].astype(float)

    def _Quantile(self):
        if self.ZhuanZaiInfo is None:
            return 
        newDf = pd.DataFrame(self.ZhuanZaiInfo,columns=["成交额(万元)","剩余规模"])
        self.QuantileDf = newDf.quantile([0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9])
        print(self.QuantileDf)
        
    def CalcVolumnScore(self):
        #计算成交量分数值
        if self.ZhuanZaiInfo is None:
            return 
        
        self.ZhuanZaiInfo["成交量分数"] = self.ZhuanZaiInfo.apply(lambda row: self._score(row["成交额(万元)"],self.QuantileDf["成交额(万元)"],False), axis=1)
        
    def CalcShengyuGuiMo(self):
        #计算市值分数值
        if self.ZhuanZaiInfo is None:
            return 
        
        self.ZhuanZaiInfo["剩余规模分数"] = self.ZhuanZaiInfo.apply(lambda row: self._score(row["剩余规模"],self.QuantileDf["剩余规模"],True), axis=1)
        

    def CalcStrongerThanIndexScore(self,startDay,endDay,columnName):
        # 技术比指数强的分数值，1.比指数抗跌，2 比指数涨的多
        sql = f'''SELECT stockID as `转债代码`,avg(flag) as {columnName} FROM stock.stockcompareindex where `date`>= '{startDay}' and `date` <= '{endDay}' group by stockID order by {columnName} DESC;'''
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)
        df.set_index(["转债代码",],drop=True,inplace=True)
       
        self.ZhuanZaiInfo[columnName] = df[columnName]
        self.ZhuanZaiInfo[f"{columnName}周期"] = f'''"{startDay}"-"{endDay}"'''

    def WriteToDB(self,date):
        if self.ZhuanZaiInfo is None:
            return
        
        for index, row in self.ZhuanZaiInfo.iterrows():
            v = row['成交量分数']
            d = row['抗跌分数']
            dd = row['抗跌分数周期']
            #m = row['市值分数']
            z = row['领涨分数']
            zz = row['领涨分数周期']
            s = row['剩余规模分数']
            
            stockID = index
            if pd.isna(z):
                z = 0
            if pd.isna(d):
                d = 0    

            total = float(v) * 0.25 + float(d)*0.25 + float(s)*0.25 + float(z)*0.25     
            sql = f'''REPLACE INTO `stock`.`kezhuanzai_score` (`日期`, `转债代码`, `成交量分数`, `抗跌分数周期`,`抗跌分数`, `领涨分数周期`,`领涨分数`,`剩余规模分数`,`总分`) VALUES ('{date}', '{stockID}', '{v}', '{dd}','{d}', '{zz}','{z}',{s},{total});'''
            #print(sql)
            self.dbConnection.Execute(sql)

    def Score(self,date,indexParams:map):
        self.GetKeZhuanZaiInfo(date)
        self._Quantile()
        self.CalcVolumnScore()
        self.CalcShengyuGuiMo()
        for key in indexParams:
            startDay = indexParams[key]["startDay"]
            endDay = indexParams[key]["endDay"]
            self.CalcStrongerThanIndexScore(startDay,endDay,key)

        self.WriteToDB(date)
    

    def Select(self,date,didian):
        sql = f'''select A.`转债代码`,B.`转债名称`,A.`总分`from kezhuanzai_score As A,kezhuanzhai AS B where A.`日期` = "{date}" and B.`日期` = "{didian}"  and A.`转债代码` = B.`转债代码` order by A.`总分` DESC;'''
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)
    
        fodler = f'{workSpaceRoot}/复盘/可转债评分/{date}/'
        if os.path.exists(fodler) == False:
            os.makedirs(fodler)
        
        fileName1 = f'''可转债评分_{date}'''
        DataFrameToJPG(df,["转债代码","转债名称","总分"],fodler,fileName1)