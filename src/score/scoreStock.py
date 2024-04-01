import pandas as pd
import numpy as np
import datetime
from workspace import GetFuPanRoot
from Utility.convertDataFrameToJPG import DataFrameToJPG

class  CScoreStock(object):
    def __init__(self,dbConnection):
        self.dbConnection = dbConnection

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

    
    def getBasicInfo(self):
        sql = f'''SELECT `股票代码`,`股票简称`,`流通市值` FROM stock.stockbasicinfo where (`股票简称` not REGEXP '退|ST' and `股票代码` not REGEXP 'BJ|^68');''' #非退市,非ST,非北交所，非科创板
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)
        df["流通市值"] = df["流通市值"].astype("float")
        df.set_index(["股票代码","股票简称"],inplace=True,drop=True)
        step = list(np.linspace(0.01, 1, 100))
        percentile = df.quantile(step)
        df["流通市值分数"] = df.apply(lambda row: self._score(row["流通市值"],percentile["流通市值"],True), axis=1)
        df.reset_index("股票简称", inplace=True)
        return df

    
    def getCompareByIndex(self,day):
        sql = f'''SELECT `stockID` as `股票代码`,`flag` as `比大盘分数` FROM stock.compareindex_stock where `date` = "{day}";'''
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)
        df.set_index("股票代码",inplace=True,drop=True)
        return df


    def _DataFrameToSqls_REPLACE_INTO(self,datas,tableName):
        sqls = []
        for _, row in datas.iterrows():
            index_str = '''`,`'''.join(row.index)
            value_str = '''","'''.join(str(x) for x in row.values)
            sql = '''REPLACE INTO {0} (`{1}`) VALUES ("{2}");'''.format(tableName,index_str,value_str)
            sqls.append(sql)
        return sqls

    def _DataFrameToSqls_UPDATE(self,datas,tableName,index_str):
        sqls = []
        for index, row in datas.iterrows():
            sql = '''UPDATE %s SET ''' %(tableName)

            for rowIndex, value in row.items():
                sql = sql + '''`%s` = '%s',''' %(rowIndex,value)
            sql = sql[:-1]
            
            if isinstance(index_str,str):
                sql = sql + ''' WHERE `%s` = '%s'; '''%(index_str,index)
            elif isinstance(index_str, (list, tuple)):
                ziped = dict(zip(index_str,index))
                indexes = ""
                for key in ziped:
                    indexes = indexes + f'''(`{key}` = '{ziped[key]}') AND '''
                
                indexes = indexes[:-4] # remove last "AND "
                sql = sql + ''' WHERE %s ; '''%(indexes)
            sqls.append(sql)
        return sqls

    def CalcVolumnPercentile(self):
        sql = f'''
        SELECT `日期`,`股票代码`,`成交量` FROM stock.stockdailyinfo_2021
        UNION ALL
        SELECT `日期`,`股票代码`,`成交量` FROM stock.stockdailyinfo_2022
        UNION ALL
        SELECT `日期`,`股票代码`,`成交量` FROM stock.stockdailyinfo_2023
        UNION ALL
        SELECT `日期`,`股票代码`,`成交量` FROM stock.stockdailyinfo
        '''
        result1,columns1 = self.dbConnection.Query(sql)
        newDf1=pd.DataFrame(result1,columns=columns1)
        groups = newDf1.groupby("股票代码")
        step = list(np.linspace(0.01, 1, 100))
        sqls = []
        for stockID, group in groups:
            df = group.set_index(["股票代码","日期"])
            df.dropna()
            df["成交量"] = df["成交量"].astype("float")

            percentile = df.quantile(step)
            percentile["百分位数"] = percentile.index
            percentile["成交量"] = percentile.apply(lambda row: '{:.02f}'.format(row["成交量"]), axis=1)
            percentile["百分位数"] = percentile.apply(lambda row: '{:.02f}'.format(row["百分位数"]), axis=1)
            percentile["股票代码"] =stockID
            percentile["更新时间"] =str(datetime.date.today())
            percentile.reset_index(drop=True)
            s = self._DataFrameToSqls_REPLACE_INTO(percentile,"percentile_stock_volumn")
            sql = " ".join(s)
            sqls.append(sql)

        step = 200
        groupedSql = [" ".join(sqls[i:i+step]) for i in range(0,len(sqls),step)]
        for sql in groupedSql:
            self.dbConnection.Execute(sql)
    
    def getBanKuaiScore(self,date):
        sql = f'''
            SELECT A.`板块代码`,B.`板块名称`,A.`股票代码`,B.`涨跌幅相对分数` FROM bankuai_stock_match AS A, (SELECT `板块代码`,`板块名称`,`涨跌幅相对分数` FROM stock.bankuai_index_score_daily where `日期` = "{date}") AS B where A.`板块代码` = B.`板块代码`
        '''
        result1,columns1 = self.dbConnection.Query(sql)
        newDf1=pd.DataFrame(result1,columns=columns1)
        newDf1["涨跌幅相对分数"] = newDf1["涨跌幅相对分数"].astype("float")
        groups = newDf1.groupby("股票代码")

        res = []
        for stockID, group in groups:
            max = 0
            banKuai = None
            for _,row in group.iterrows():
                score = row["涨跌幅相对分数"]
                banKuaiID = row["板块代码"]
                banKuaiName = row["板块名称"]
                if score > max:
                    max  = score
                    banKuai = banKuaiID
            res.append([stockID,banKuai,banKuaiName,max])   

        df = pd.DataFrame(res,columns = ["股票代码","板块代码","板块名称","板块分数"])
        df.set_index("股票代码",inplace=True,drop=True)
        return df
    

    def getVolumnScore(self,date):
        sql = f'''SELECT `日期`,`股票代码`,`成交量` FROM stock.stockdailyinfo where `日期` = "{date}";'''
        result1,columns1 = self.dbConnection.Query(sql)
        newDf1=pd.DataFrame(result1,columns=columns1)
        
        sql2 = f'''SELECT `股票代码`,`百分位数`,`成交量` FROM stock.percentile_stock_volumn;'''
        result2,columns2 = self.dbConnection.Query(sql2)
        newDf2=pd.DataFrame(result2,columns=columns2)
        newDf2["成交量"] = newDf2["成交量"].astype(float)
        percentileDF = newDf2.groupby("股票代码")

        for index,row in newDf1.iterrows():
            stockID = row["股票代码"]
            volumn = row["成交量"]
            percentile = percentileDF.get_group(stockID)
            
            percentile.set_index("百分位数",drop=True,inplace=True)
            newDf1.loc[index, '成交量分数'] = self._score(volumn,percentile["成交量"],False)
        newDf1.set_index("股票代码",inplace=True,drop=True)
        return newDf1

    def SelectTop80(self,date):
        sql1 = f'''SELECT * FROM stock.stock_score_daily where `日期` = "{date}" order by `总分1` DESC limit 80;'''
        result1,columns1 = self.dbConnection.Query(sql1)
        newDf1=pd.DataFrame(result1,columns=columns1)

        sql2 = f'''SELECT * FROM stock.stock_score_daily where `日期` = "{date}" order by `总分1` DESC limit 80;'''
        result2,columns2 = self.dbConnection.Query(sql2)
        newDf2=pd.DataFrame(result2,columns=columns2)

        sql3 = f'''SELECT * FROM stock.stock_score_daily where `日期` = "{date}" order by `总分1` DESC limit 80;'''
        result3,columns3 = self.dbConnection.Query(sql3)
        newDf3=pd.DataFrame(result3,columns=columns3)

        root = GetFuPanRoot(date)
        DataFrameToJPG(newDf1,["股票代码","股票简称"],root,"自动选股1")
        DataFrameToJPG(newDf2,["股票代码","股票简称"],root,"自动选股2")
        DataFrameToJPG(newDf3,["股票代码","股票简称"],root,"自动选股3")

        df1 = pd.DataFrame(newDf1,columns = ["日期","股票代码","股票简称"])
        df1["名称"] = "自动选股_25_25_25_25"
        sqls = self._DataFrameToSqls_REPLACE_INTO(df1,"simulate_trading")

        df2 = pd.DataFrame(newDf2,columns = ["日期","股票代码","股票简称"])
        df2["名称"] = "自动选股_40_10_10_40"
        sqls2 = self._DataFrameToSqls_REPLACE_INTO(df2,"simulate_trading")   

        df3 = pd.DataFrame(newDf3,columns = ["日期","股票代码","股票简称"])
        df3["名称"] = "自动选股_30_10_20_40"
        sqls3 = self._DataFrameToSqls_REPLACE_INTO(df3,"simulate_trading")

        all = sqls+ sqls2 + sqls3
        sql = " ".join(all)
        self.dbConnection.Execute(sql)

    def FillShouYi(self):
        sql1 = f'''SELECT * FROM stock.simulate_trading where `买入日期` is NULL or `买入价格` = "nan" or `1日后卖出收益` = "nan" or `3日后卖出收益` = "nan" or `5日后卖出收益` = "nan";'''
        result,columns = self.dbConnection.Query(sql1)
        newDf = pd.DataFrame(result,columns=columns)
 
        miniDate = min(newDf["日期"])
        sql2 = f'''SELECT * FROM stock.stockdailyinfo where `股票代码` in (SELECT distinct(`股票代码`) FROM stock.simulate_trading where `买入日期` is NULL or `买入价格` = "nan" or `1日后卖出收益` = "nan" or `3日后卖出收益` = "nan" or `5日后卖出收益` = "nan") and `日期` >="{miniDate}";'''
        result1,columns1 = self.dbConnection.Query(sql2)
        newDf1=pd.DataFrame(result1,columns=columns1)

        sqls = []
        for index, row in newDf.iterrows():
            stockId = row["股票代码"]
            date = row["日期"]
            name = row["名称"]

            df = newDf1[(newDf1["股票代码"] == stockId) & (newDf1["日期"] >= date)].copy()
            df['开盘价'] = df['开盘价'].astype(float)
            df["买入日期"] = df['日期'].shift(-1)
            df["买入价格"] = df['开盘价'].shift(-1)

            df["第3天开盘价"] = df['开盘价'].shift(-2)
            df["第5天开盘价"] = df['开盘价'].shift(-4)
            df["第7天开盘价"] = df['开盘价'].shift(-6)

            df["1日后卖出收益"] = (df["第3天开盘价"] - df['买入价格']) / df['买入价格'] * 100
            df["3日后卖出收益"] = (df["第5天开盘价"] - df['买入价格']) / df['买入价格'] * 100
            df["5日后卖出收益"] = (df["第7天开盘价"] - df['买入价格']) / df['买入价格'] * 100

            df["1日后卖出收益"] = df.apply(lambda row: '{:.02f}'.format(row["1日后卖出收益"]), axis=1)
            df["3日后卖出收益"] = df.apply(lambda row: '{:.02f}'.format(row["3日后卖出收益"]), axis=1)
            df["5日后卖出收益"] = df.apply(lambda row: '{:.02f}'.format(row["5日后卖出收益"]), axis=1)
            df["名称"] = name

            resultDf = pd.DataFrame(df,columns = ["日期","名称","股票代码","买入日期","买入价格","1日后卖出收益","3日后卖出收益","5日后卖出收益"])
            resultDf.set_index(["日期","名称","股票代码"],drop=True,inplace=True)
            resultDf = resultDf[resultDf["买入日期"].notnull()]
            sqls1 = self._DataFrameToSqls_UPDATE(resultDf,"simulate_trading",["日期","名称","股票代码"])
            sqls.extend(sqls1)

        step = 400
        groupedSql = [" ".join(sqls[i:i+step]) for i in range(0,len(sqls),step)]
        for sql in groupedSql:
            self.dbConnection.Execute(sql)

    def Score(self,date):
        basickInfo = self.getBasicInfo()
        biDaPan = self.getCompareByIndex(date)
        volumnPercentile = self.getVolumnScore(date)
        banKuaiScoreDF = self.getBanKuaiScore(date)
        df1 = pd.merge(basickInfo,biDaPan,on = "股票代码")
        df2 = pd.merge(df1,volumnPercentile,on = "股票代码")
        df3 = pd.merge(df2,banKuaiScoreDF,on = "股票代码")
        df3.dropna()
        df3["总分1"] = 0.25*df3["板块分数"] + 0.25*df3["成交量分数"] + 0.25*df3["流通市值分数"] + 0.25*df3["比大盘分数"]  #
        df3["总分2"] = 0.4*df3["板块分数"] + 0.1*df3["成交量分数"] + 0.1*df3["流通市值分数"] + 0.4*df3["比大盘分数"]
        df3["总分3"] = 0.3*df3["板块分数"] + 0.1*df3["成交量分数"] + 0.2*df3["流通市值分数"] + 0.4*df3["比大盘分数"]
        df3.reset_index(inplace=True)
        resultDf = pd.DataFrame(df3,columns=["日期","股票代码","股票简称","板块代码","板块名称","板块分数","成交量分数","流通市值分数","比大盘分数","总分1","总分2","总分3"])
        resultDf["总分1"] = resultDf.apply(lambda row: '{:.02f}'.format(row["总分1"]), axis=1)
        resultDf["总分2"] = resultDf.apply(lambda row: '{:.02f}'.format(row["总分2"]), axis=1)
        resultDf["总分3"] = resultDf.apply(lambda row: '{:.02f}'.format(row["总分3"]), axis=1)
        
        sqls = self._DataFrameToSqls_REPLACE_INTO(resultDf,"stock_score_daily")
        step = 400
        groupedSql = [" ".join(sqls[i:i+step]) for i in range(0,len(sqls),step)]
        for sql in groupedSql:
            self.dbConnection.Execute(sql)
        
        self.SelectTop80(date)
        self.FillShouYi()
        






