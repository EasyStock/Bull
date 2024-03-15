import os
import pandas as pd
import numpy as np
from workspace import workSpaceRoot,WorkSpaceFont,GetZhuanZaiFolder
from Utility.convertDataFrameToJPG import DataFrameToJPG

class CScoreZaiMgr(object):
    def __init__(self,dbConnection,date) -> None:
        self.dbConnection = dbConnection
        self.ZhuanZaiInfo = None
        self.QuantileDf = None
        self.date = date

    def _score(self,volumn,percentail,reversed = False):
        result = 1
        for index, value in percentail.items():
            if float(volumn) <= float(value):
                result = float(index)
                break
        
        if reversed:
            result = 1.0 - float(result)
        
        result = int(result * 100)
        return result

    def GetKeZhuanZaiInfo(self):
        sql = f'''select * FROM stock.kezhuanzhai_all where `日期` = "{self.date}";'''
        results, columns = self.dbConnection.Query(sql)
        self.ZhuanZaiInfo = pd.DataFrame(results,columns=columns)
        self.ZhuanZaiInfo.set_index(["转债代码",],drop=True,inplace=True)
        self.ZhuanZaiInfo['剩余规模'] = self.ZhuanZaiInfo['剩余规模'].astype(float)

        sql = f'''SELECT stockID as `转债代码`,`delta` as `涨幅差`  FROM stock.compareindex_zai where `date`= '{self.date}';'''
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)
        df.set_index(["转债代码",],drop=True,inplace=True)
        self.ZhuanZaiInfo["涨幅差"] = df["涨幅差"]

    def _Quantile(self):
        if self.ZhuanZaiInfo is None:
            return 
        newDf = pd.DataFrame(self.ZhuanZaiInfo,columns=["成交额(万元)","剩余规模","涨幅差"])
        step = list(np.linspace(0, 1, 101))
        self.QuantileDf = newDf.quantile(step)
        print(self.QuantileDf)
        
    def CalcVolumnScore(self):
        #计算成交量分数值
        if self.ZhuanZaiInfo is None:
            return 
        
        self.ZhuanZaiInfo["成交量分数"] = self.ZhuanZaiInfo.apply(lambda row: self._score(row["成交额(万元)"],self.QuantileDf["成交额(万元)"],False), axis=1)
        
    def CalcShengyuGuiMo(self):
        #计算剩余规模分数值
        if self.ZhuanZaiInfo is None:
            return 
        
        self.ZhuanZaiInfo["剩余规模分数"] = self.ZhuanZaiInfo.apply(lambda row: self._score(row["剩余规模"],self.QuantileDf["剩余规模"],True), axis=1)
        
    def CalcStrongerThanIndexScore(self):
        # 技术比指数强的分数值，1.比指数抗跌，2 比指数涨的多
        if self.ZhuanZaiInfo is None:
            return 
        
        self.ZhuanZaiInfo["比指数分数"] = self.ZhuanZaiInfo.apply(lambda row: self._score(row["涨幅差"],self.QuantileDf["涨幅差"],False), axis=1)
        

    def WriteToDB(self):
        if self.ZhuanZaiInfo is None:
            return
        
        for index, row in self.ZhuanZaiInfo.iterrows():
            v = row.get('成交量分数',0)
            d = row.get('比指数分数',0)
            s = row.get('剩余规模分数',0)

            stockID = index
            if pd.isna(d):
                d = 0    

            total = float(v) * 0.333 + float(d)*0.333 + float(s)*0.3333
            sql = f'''REPLACE INTO `stock`.`kezhuanzai_score_everyday` (`日期`, `转债代码`, `成交量分数`,`比指数分数`,`剩余规模分数`,`总分`) VALUES ('{self.date}', '{stockID}', '{v}', '{d}',{s},{total:.1f});'''
            self.dbConnection.Execute(sql)

    def Score(self):
        self.GetKeZhuanZaiInfo()
        self._Quantile()
        self.CalcVolumnScore()
        self.CalcShengyuGuiMo()
        self.CalcStrongerThanIndexScore()
        self.WriteToDB()



class CSelectZai(object):
    def __init__(self,dbConnection,tradingDays):
        self.dbConnection = dbConnection
        self.tradingDays = tradingDays
        self.allBasicInfo = None
        self.matchedInfo = None
        self.basicInfoColumns = None

    def GetBasicInfo(self):
        sql = f'''SELECT `转债代码`,`转债名称`,`现价`,`成交额(万元)` ,`PB`,`溢价率`,`剩余年限`,`剩余规模`,`到期税前收益率` FROM stock.kezhuanzhai_all where `日期`="{self.tradingDays[-1]}";'''
        results, columns = self.dbConnection.Query(sql)
        self.basicInfoColumns = columns
        self.allBasicInfo = pd.DataFrame(results,columns = columns)

    def GetMatchedInfo(self):
        sql = f'''SELECT `转债代码`,`转债名称` FROM stock.kezhuanzhai where `日期`="{self.tradingDays[-1]}";'''
        results, columns = self.dbConnection.Query(sql)
        self.matchedInfo = pd.DataFrame(results,columns = columns)

    def _isMatched(self,stockID):
        matched = list(self.matchedInfo['转债代码'])
        if stockID in matched:
            return True
        
        return False

    def formatVolumn(self,volumn):
        ret = f'''{volumn:.2f}'''
        return ret
    
    def Select(self,param:map):
        self.GetBasicInfo()
        self.GetMatchedInfo()
        datas = []
        columns = None
        for key in param:
            v = param[key]
            startDay = v.get("startDay",None)
            endDay = v.get("endDay",None)
            if startDay is not None and endDay is not None:
                sql = f'''SELECT * FROM stock.kezhuanzai_score_everyday where `日期`>="{startDay}" and `日期`<="{endDay}";'''
            elif startDay is not None:
                sql = f'''SELECT * FROM stock.kezhuanzai_score_everyday where `日期`="{startDay}";'''
            elif endDay is not None:
                sql = f'''SELECT * FROM stock.kezhuanzai_score_everyday where `日期`="{endDay}";'''
            
            results, columns = self.dbConnection.Query(sql)
            datas.extend(results)
        
        datas = list(set(datas))
        df = pd.DataFrame(datas,columns = columns)
        res = []
        groups = df.groupby("转债代码")
        for stockID, group in groups:
            avg = group["总分"].mean()
            s = group["总分"].std()
            res.append({"转债代码":stockID,"平均分":avg,"方差":s})
        
        resDf = pd.DataFrame(res, columns=["转债代码", "平均分",'方差'])
    
        resDf["符合条件"] = resDf.apply(lambda row: self._isMatched(row['转债代码']), axis=1)
        resDf["平均分"] = resDf.apply(lambda row: self.formatVolumn(row['平均分']), axis=1)
        resDf["方差"] = resDf.apply(lambda row: self.formatVolumn(row['方差']), axis=1)
        resDf = pd.merge(resDf,self.allBasicInfo, how='inner',left_on=("转债代码",),right_on=("转债代码",))
        
        resDf.sort_values(['平均分',"溢价率"],axis=0,ascending=False,inplace=True)
        columns = self.basicInfoColumns + ["平均分","符合条件"]
        result = pd.DataFrame(resDf,columns = columns)
        

        fodler = GetZhuanZaiFolder(self.tradingDays[-1])
        fileName1 = f'''可转债评分EX_符合条件_带分数'''
        fileName2 = f'''可转债评分EX_符合条件_THS'''

        xlsxFileName = os.path.join(fodler,f"可转债评分EX_全_{self.tradingDays[-1]}.xlsx")
        xlsxFileName_Matched = os.path.join(fodler,f"可转债评分EX_符合条件的_{self.tradingDays[-1]}.xlsx")
        result.to_excel(xlsxFileName,index=False)

        matchedDf = result[result["符合条件"]]
        matchedDf.reset_index(inplace=True)
        matchedDf.to_excel(xlsxFileName_Matched,index=False)
        DataFrameToJPG(matchedDf,["转债代码","转债名称","平均分"],fodler,fileName1)
        DataFrameToJPG(matchedDf,["转债代码","转债名称"],fodler,fileName2)




    
