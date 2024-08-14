import datetime
import pytz
import pandas as pd
from Utility.convertDataFrameToJPG import DataFrameToJPG
from workspace import GetStockFolder
from mysql.connect2DB import DataFrameToSqls_REPLACE
import re

class CStockPattern12(object):
    '''
    51020上穿战法
    1. 在10个交易日内
    2. 同时出现下面几个信号
        2.1. 5日线上穿10日线
        2.2. 5日线上穿20日线
        2.3. 1日线上穿20日线 

    '''
    def __init__(self,dbConnection,lastNDays = 200):
        self.dbConnection = dbConnection
        self.lastNDays = lastNDays
        self.N = 10
    
    def GetTradingDates(self):
        today = datetime.datetime.now(pytz.timezone('Asia/Shanghai')).date()
        end = today.strftime("%Y-%m-%d")
        sql = f"SELECT `日期` FROM stock.treadingDay where `开市` =1 and `交易所`='SSE' and `日期`<='{end}' order by `日期` DESC limit {self.lastNDays};"
        res,_ = self.dbConnection.Query(sql)
        self.tradingDays = [r[0] for r in reversed(res)]
        return self.tradingDays


        
    def _GetStockData(self,startDay,stockID = None):
        sql1 = f'''
        SELECT B.`日期`,A.`股票代码`,A.`股票简称` As `股票名称` ,B.`开盘价`,B.`收盘价` ,B.`最低价`,B.`最高价`,B.`成交量` FROM stock.stockbasicinfo AS A,(
            SELECT `日期`,`股票代码`,`开盘价`,`收盘价`,`最低价`,`最高价`,`成交量` FROM stock.stockdailyinfo_2022 where `日期` >= "{startDay}" 
            UNION 
            SELECT `日期`,`股票代码`,`开盘价`,`收盘价`,`最低价`,`最高价`,`成交量` FROM stock.stockdailyinfo_2023 where `日期` >= "{startDay}" 
            UNION 
            SELECT `日期`,`股票代码`,`开盘价`,`收盘价` ,`最低价`,`最高价`,`成交量` FROM stock.stockdailyinfo where `日期` >= "{startDay}"
            ) AS B where A.`股票代码` = B.`股票代码` 
        '''
        if stockID is not None:
            sql1 = f'''{sql1} and A.`股票代码` = "{stockID}" '''
        result1,columns1 = self.dbConnection.Query(sql1)
        newDf1=pd.DataFrame(result1,columns=columns1)
        return newDf1
    
    def _FilterLast(self,stockID,stockName,df:pd.DataFrame,today):
        df = df.reset_index(drop=True)
        if df.shape[0] <= 20:
            return None
    
        df.dropna(inplace=True)
        df["开盘价"] = df["开盘价"].astype("float")
        df["收盘价"] = df["收盘价"].astype("float")
        df["最低价"] = df["最低价"].astype("float")
        df["最高价"] = df["最高价"].astype("float")
        df["成交量"] = df["成交量"].astype("float")
        df["MA5"] = df["收盘价"].rolling(window=5).mean()
        df["MA10"] = df["收盘价"].rolling(window=10).mean()
        df["MA20"] = df["收盘价"].rolling(window=20).mean()
        df["MA5 - MA10"] = df["MA5"] - df["MA10"]
        df["MA5 - MA20"] = df["MA5"] - df["MA20"]
        df["MA10 - MA20"] = df["MA10"] - df["MA20"]
        df["MA5 - MA10_Yesterday"] =df["MA5 - MA10"].shift(1)
        df["MA5 - MA20_Yesterday"] = df["MA5 - MA20"].shift(1)
        df["MA10 - MA20_Yesterday"] = df["MA10 - MA20"].shift(1)
        df["MA5上穿MA10"] = (df["MA5 - MA10_Yesterday"] < 0) & (df["MA5 - MA10"] > 0)
        df["MA5上穿MA20"] = (df["MA5 - MA20_Yesterday"] < 0) & (df["MA5 - MA20"] > 0)
        df["MA10上穿MA20"] = (df["MA10 - MA20_Yesterday"] < 0) & (df["MA10 - MA20"] > 0)
        df["MA5下穿MA10"] = (df["MA5 - MA10_Yesterday"] > 0) & (df["MA5 - MA10"] < 0)

        lastNDf = df[-self.N:]
        result = None
        if lastNDf["MA5上穿MA10"].sum() >=1 and lastNDf["MA5上穿MA20"].sum() >=1  and lastNDf["MA10上穿MA20"].sum() >=1:
            row = lastNDf[lastNDf["MA5上穿MA10"]].iloc[-1]

            if row["MA5"] > row["MA20"]:
                return None  # MA5上穿MA10 穿10日这一天，5日线应该在20日线下面
            
            day5_10 = lastNDf[lastNDf["MA5上穿MA10"]].iloc[-1]["日期"]
            day5_20 = lastNDf[lastNDf["MA5上穿MA20"]].iloc[-1]["日期"]
            day10_20 = lastNDf[lastNDf["MA10上穿MA20"]].iloc[-1]["日期"]
             


            result = {}
            result["日期"] = max(day5_10,day5_20,day10_20)
            result["股票代码"] = stockID
            result["股票名称"] = stockName
            if day5_10 <= day5_20 and day5_20 <= day10_20:
                result["战法名称"] = f'''51020上穿战法'''
            else:
                result["战法名称"] = f'''51020上穿战法*'''

            result["MA5上穿MA10"] = day5_10
            result["MA5上穿MA20"] = day5_20
            result["MA10上穿MA20"] = day10_20
            result["其他信息"] = " ".join([
                day5_10,
                day5_20,
                day10_20,
            ])
                                          
        return result

    def _FilterAllStocks(self,allDataDf:pd.DataFrame,today):
        groups = allDataDf.groupby(["股票代码","股票名称"])
        results = []
        for (stockID,stockName), group in groups:
            res = self._FilterLast(stockID,stockName,group,today)
            if res is not None:
                results.append(res)
        
        return results
    
         
    def SelectLast(self,stockID = None):
            days = 30
            tradingDays = self.GetTradingDates()
            tradingDays = tradingDays[-days:]
            df = self._GetStockData(tradingDays[0],stockID)
            results = []
            res = self._FilterAllStocks(df,tradingDays[-1])
            results.extend(res)

            resultDf = pd.DataFrame(results)
            if not resultDf.empty:
                resultDf = resultDf[resultDf['股票名称'].str.match('[\s\S]*(ST|退|C)+?[\s\S]*') == False]
                resultDf = resultDf[resultDf['股票代码'].str.match('[\s\S]*(BJ|^688)+?[\s\S]*') == False]
            root = GetStockFolder(tradingDays[-1])
            resultDf.sort_values(by=["日期"], inplace=True,ascending=False)
            resultDf.reset_index(drop=True,inplace=True)
            resultDf.to_excel(f'''{root}/51020上穿战法.xlsx''',index=False)

            newDF = pd.DataFrame(resultDf, columns = ["日期","股票代码","股票名称","战法名称","其他信息"])
            sqls = DataFrameToSqls_REPLACE(newDF,"zhanfa_test")
            if len(sqls) > 0:
                sql = " ".join(sqls)
                self.dbConnection.Execute(sql)

            # sql1 = f'''SELECT `日期`,`股票代码`,`股票名称` FROM stock.zhanfa where `日期` = "{today}" and   `战法名称` = "N形战法" and `股票代码` not in (SELECT `股票代码` FROM stock.zhanfa where `日期` = "{yesterday}")'''
            # result1,columns1 = self.dbConnection.Query(sql1)
            # newDf1=pd.DataFrame(result1,columns=columns1)

            #DataFrameToJPG(newDf1,("股票代码","股票名称"),root,"N形战法_增量")
            DataFrameToJPG(resultDf,("股票代码","股票名称"),root,f'''51020上穿战法''')

            print(resultDf)

    def SelectAll(self,stockID = None):
            days = 800
            tradingDays = self.GetTradingDates()
            tradingDays = tradingDays[-days:]
            df = self._GetStockData(tradingDays[0],stockID)
            results = []
            for index in range(1,len(tradingDays) - self.N):
                today = tradingDays[-index]
                #print(f'''{index}, {today}, total:{days - self.N}''')
                newDF = df[df["日期"] <= today]
                res = self._FilterAllStocks(newDF,today)

                resultDf = pd.DataFrame(res)
                if not resultDf.empty:
                    resultDf = resultDf[resultDf['股票名称'].str.match('[\s\S]*(ST|退|C)+?[\s\S]*') == False]
                    resultDf = resultDf[resultDf['股票代码'].str.match('[\s\S]*(BJ|^688)+?[\s\S]*') == False]

                newDF = pd.DataFrame(resultDf, columns = ["日期","股票代码","股票名称","战法名称","其他信息"])
                sqls = DataFrameToSqls_REPLACE(newDF,"zhanfa_test")
                if len(sqls) > 0:
                    sql = " ".join(sqls)
                    self.dbConnection.Execute(sql)
                results.extend(res)

            if len(results) == 0:
                return
            
            resultDf = pd.DataFrame(results)
            resultDf.drop_duplicates(inplace=True)
            if not resultDf.empty:
                resultDf = resultDf[resultDf['股票名称'].str.match('[\s\S]*(ST|退|C)+?[\s\S]*') == False]
                resultDf = resultDf[resultDf['股票代码'].str.match('[\s\S]*(BJ|^688)+?[\s\S]*') == False]
            
            
            resultDf.sort_values(by=["日期"], inplace=True,ascending=False)
            resultDf.reset_index(drop=True,inplace=True)

            # root = GetStockFolder(tradingDays[-1])
            # resultDf.to_excel(f'''{root}/51020上穿战法.xlsx''',index=False)



            # sql1 = f'''SELECT `日期`,`股票代码`,`股票名称` FROM stock.zhanfa where `日期` = "{today}" and   `战法名称` = "N形战法" and `股票代码` not in (SELECT `股票代码` FROM stock.zhanfa where `日期` = "{yesterday}")'''
            # result1,columns1 = self.dbConnection.Query(sql1)
            # newDf1=pd.DataFrame(result1,columns=columns1)

            #DataFrameToJPG(newDf1,("股票代码","股票名称"),root,"N形战法_增量")
            #DataFrameToJPG(resultDf,("股票代码","股票名称"),root,f'''51020上穿战法''')
            print(resultDf)

    def FormatFillPercentagesSql(self,df,groupRow):
        date = groupRow["日期"]
        newDf = df[df["日期"] > date].copy()
        if newDf.empty:
            return None
        
        newDf.reset_index(drop=True,inplace=True)
        buyPrice =  newDf["开盘价"].iloc[0] # 第二天开盘价即为买入价
        shape = newDf.shape[0]
        N = [3, 5, 10, 20, 30]
        partSqls = []
        for n in N:
            if shape < n:
                continue
            row = newDf.iloc[n-1]
            key = f'''{n}日后涨幅(%)'''
            open = row["开盘价"]
            zhangDieFu = (open - buyPrice) / buyPrice *100
            sql_part = f'''`{key}` = '{zhangDieFu:.2f}' '''
            partSqls.append(sql_part)
        
        newDf_30 = newDf[:30]
        max_row_index = newDf_30['开盘价'].idxmax()
        max_row = newDf_30.iloc[max_row_index]
        max_zhagnfu = (max_row["开盘价"] - buyPrice)/ buyPrice *100

        min_row_index = newDf_30['开盘价'].idxmin()
        min_row = newDf_30.iloc[min_row_index]
        min_zhagnfu = (min_row["开盘价"] - buyPrice)/ buyPrice *100

        avg_zhagnfu = (newDf_30["开盘价"].mean() - buyPrice)/ buyPrice *100

        resultSql = f'''UPDATE `stock`.`zhanfa_test` SET `30天内最高点天数` = '{max_row_index + 1}', `30天内最高涨幅(%)` = '{max_zhagnfu:.2f}', `30天内最低点天数` = '{min_row_index + 1}', `30天内最低涨幅(%)` = '{min_zhagnfu:.2f}', `30天内平均涨幅(%)` = '{avg_zhagnfu:.2f}' WHERE (`日期` = '{groupRow["日期"]}') and (`股票代码` = '{groupRow["股票代码"]}') and (`战法名称` = '{groupRow["战法名称"]}');'''
        if len(partSqls) != 0:
            partSql = " , ".join(partSqls)
            resultSql = f'''UPDATE `zhanfa_test` SET {partSql} ,`30天内最高点天数` = '{max_row_index + 1}', `30天内最高涨幅(%)` = '{max_zhagnfu:.2f}', `30天内最低点天数` = '{min_row_index + 1}', `30天内最低涨幅(%)` = '{min_zhagnfu:.2f}', `30天内平均涨幅(%)` = '{avg_zhagnfu:.2f}' WHERE (`日期` = '{groupRow["日期"]}') and (`股票代码` = '{groupRow["股票代码"]}') and (`战法名称` = '{groupRow["战法名称"]}');'''

        #print(resultSql)
        #print(newDf)
        #newDf.to_excel(f'''/tmp/{date}.xlsx''')
        return resultSql

        
    def FillPercentages(self):
        sql = f'''SELECT * FROM stock.zhanfa_test where `3日后涨幅(%)` is NULL or `5日后涨幅(%)` is NULL or `10日后涨幅(%)` is NULL or `20日后涨幅(%)` is NULL  or `30日后涨幅(%)` is NULL;'''
        result,columns = self.dbConnection.Query(sql)
        newDf =pd.DataFrame(result,columns=columns)
        groups = newDf.groupby(["股票代码","股票名称"])
        for (stockID,stockName), group in groups:
            minDay = group["日期"].min()
            sql1 = f'''SELECT `日期`,`股票代码`,`开盘价`,`收盘价` ,`最低价`,`最高价`,`成交量` FROM stock.stockdailyinfo where `股票代码` = "{stockID}" and `日期` > "{minDay}"  UNION SELECT `日期`,`股票代码`,`开盘价`,`收盘价` ,`最低价`,`最高价`,`成交量` FROM stock.stockdailyinfo_2023 where `股票代码` = "{stockID}" and `日期` > "{minDay}" order by `日期`;'''
            result1,columns1 = self.dbConnection.Query(sql1)
            newDf1=pd.DataFrame(result1,columns=columns1)
            if newDf1.empty:
                continue

            newDf1['开盘价'] = newDf1['开盘价'].astype(float)
            newDf1['收盘价'] = newDf1['收盘价'].astype(float)
            newDf1['最低价'] = newDf1['最低价'].astype(float)
            newDf1['最高价'] = newDf1['最高价'].astype(float)
            newDf1['成交量'] = newDf1['成交量'].astype(float)
            sqls = []
            for _, row in group.iterrows():
                retSql = self.FormatFillPercentagesSql(newDf1,row)
                if retSql is not None:
                    sqls.append(retSql)
            sql = " ".join(sqls)
            self.dbConnection.Execute(sql)

            # print(newDf1)
            # newDf1.to_excel("/tmp/111.xlsx")


    def UpdatedateShenglvPeilv(self):
        sql = f'''SELECT * FROM stock.zhanfa_test where 战法名称 like "%51020上穿战法%"; '''
        result,columns = self.dbConnection.Query(sql)
        newDf =pd.DataFrame(result,columns=columns)  
        groups = newDf.groupby(["股票代码","股票名称"])
        N = [3, 5, 10, 20, 30]
        sqls = []
        for (stockID,stockName), group in groups:
            if group.empty:
                continue

            keys = ["战法名称","股票代码"]
            values = [group["战法名称"].iloc[0],group["股票代码"].iloc[0]]
            for n in N:
                key1 = f'''{n}日胜率(%)'''
                key2 = f'''{n}日赔率(%)'''
                key3 = f'''{n}日后涨幅(%)'''
                row = group[key3].dropna()
                if row.empty:
                    continue

                size = row.shape[0]
                shenglv = f'''{row[row > 0].shape[0] / size*100 :.2f}'''
                peilv = f'''{row.mean():.2f}'''
                keys.extend((key1,key2))
                values.extend((shenglv,peilv))
            
            keyStr = "`, `".join(keys)
            valuesStr = "', '".join(values)
            sql = f'''Replace INTO `stock`.`zhanfa_result` (`{keyStr}`) VALUES ('{valuesStr} ');'''
            sqls.append(sql)
        
        step = 200
        grouped_sqls = [" ".join(sqls[i:i+step]) for i in range(0,len(sqls),step)]
        for sql in grouped_sqls:
            self.dbConnection.Execute(sql)
    

    def _applyLastData(self,df,row, tradingDays):
            stockID = row["股票代码"]
            otherDf = df[(df["股票代码"] == stockID)]
            otherDf = otherDf[~otherDf["日期"].isin(tradingDays)]
            otherInfo = "\n".join(list(otherDf["其他信息"]))
            return otherInfo
    
    def _applyLastDataCount(self,df,row, tradingDays):
            stockID = row["股票代码"]
            otherDf = df[(df["股票代码"] == stockID)]
            otherDf = otherDf[~otherDf["日期"].isin(tradingDays)]
            return otherDf["其他信息"].count()
    
    def SelectLastDayData(self):
        tradingDays = self.GetTradingDates()
        tradingDays = tradingDays[-1:]
        sql = f'''select * from (SELECT * FROM stock.zhanfa_test where `日期` in ("{ '","'.join(tradingDays)}") ) AS A,(SELECT * FROM stock.zhanfa_result ) AS B where A.`战法名称` = B.`战法名称` and A.`股票代码` = B.`股票代码` and A.`战法名称` like "%51020上穿战法%" order by A.`日期` DESC,B.`3日胜率(%)`DESC ,B.`3日赔率(%)` DESC;'''
        print(sql)
        result,columns = self.dbConnection.Query(sql)
        newDf =pd.DataFrame(result,columns=columns)
        newDf = newDf.loc[:, ~newDf.columns.duplicated()]

        sql2  = f'''SELECT * FROM stock.zhanfa_test where `战法名称` like "%51020上穿战法%"; '''
        result2,columns2 = self.dbConnection.Query(sql2)
        newDf2 =pd.DataFrame(result2,columns=columns2)
  
        newDf['过往记录数量'] = newDf.apply(lambda row: self._applyLastDataCount(newDf2,row,tradingDays), axis=1)
        newDf['过往记录'] = newDf.apply(lambda row: self._applyLastData(newDf2,row,tradingDays), axis=1)

        newDf.sort_values(by=["日期","3日胜率(%)","3日赔率(%)","过往记录数量"], ascending=[False,False,False,False], inplace=True)
        newDf.to_excel("/tmp/lastDayData.xlsx",columns=["股票代码","股票名称","3日胜率(%)","3日赔率(%)","过往记录数量","过往记录","5日胜率(%)","5日赔率(%)","10日胜率(%)","10日赔率(%)","20日胜率(%)","20日赔率(%)","30日胜率(%)","30日赔率(%)","日期","战法名称","其他信息"])
        #root = GetStockFolder(tradingDays[-1])
        root = "/tmp/"
        name = f'''51020上穿战法_{tradingDays[-1]}'''
        DataFrameToJPG(newDf,("股票代码","股票名称"),root,name)