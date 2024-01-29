#!/usr/bin/env python3
import pandas as pd

class CResetVMAData(object):
    def __init__(self,dbConnection):
        self.dbConnection = dbConnection

    def MergeTables(self):
        tables = [
            "stockdailyinfo_2021",
            "stockdailyinfo_2022",
            "stockdailyinfo_2023",
            "stockdailyinfo",
        ]
        for tableName in tables:
            sql = f'''"INSERT INTO `stockdailyinfo_traning` (`日期`,`股票代码`,`开盘价`,`收盘价`,`最高价`,`最低价`,`成交量`,`成交额`,`涨跌幅`,`V/MA60`,`1日后涨幅`,`3日后涨幅`,`5日后涨幅`,`7日后涨幅`) SELECT `日期`,`股票代码`,`开盘价`,`收盘价`,`最高价`,`最低价`,`成交量`,`成交额`,`涨跌幅`,`V/MA60`,`1日后涨幅`,`3日后涨幅`,`5日后涨幅`,`7日后涨幅` FROM stock.{tableName}",'''
            self.dbConnection.Execute(sql)
    

    def CleanTables(self):
        sqls = [
            "TRUNCATE `stock`.`stockdailyinfo_traning`;",
            "TRUNCATE `stock`.`stockdailyinfo_traning_result`;",
        ]
        for sql in sqls:
            self.dbConnection.Execute(sql)
           
    def CleanData(self):
        sql = f'''DELETE from  `stock`.`stockdailyinfo_traning` where `开盘价` like "%-%" or `收盘价` like "%-%" or `最高价` like "%-%" or `最低价` like "%-%" or `成交量` like "%-%" or `成交额` like "%-%" '''
        self.dbConnection.Execute(sql)

    def Reset(self):
        self.CleanTables()  #清空两表
        self.MergeTables()  #合并数据
        self.CleanData()    #清洗数据，去除异常数据

class CUpdateVMAData(object):
    def __init__(self,dbConnection,stockID):
        self.dbConnection = dbConnection
        self.stockID = stockID
        self.df = None
    
    def GetTraingData(self):
        sql = f'''SELECT * FROM stock.stockdailyinfo_traning where `股票代码` = "{self.stockID}";'''
        results, columns = self.dbConnection.Query(sql)
        self.df = pd.DataFrame(results,columns=columns)
        self.df['成交量'] = self.df['成交量'].astype(float)
        self.df['开盘价'] = self.df['开盘价'].astype(float)
    
    def GetDailyData(self):
        sql = f'''
        SELECT * FROM stock.stockdailyinfo_2023 where `股票代码` = "{self.stockID}"

        UNION ALL

        SELECT * FROM stock.stockdailyinfo where `股票代码` = "{self.stockID}" ;
        '''
        results, columns = self.dbConnection.Query(sql)
        self.df = pd.DataFrame(results,columns=columns)
        self.df['成交量'] = self.df['成交量'].astype(float)
        self.df['开盘价'] = self.df['开盘价'].astype(float)

    def _setVMAN(self,N):
        key1 = f'''V_MA{N}'''
        key2 = f'''V/MA{N}'''
        self.df[key1] = self.df['成交量'].rolling(N).mean()
        self.df[key2] = self.df.apply(lambda row: '{:.2f}'.format(row['成交量']/row[key1]), axis=1) 

    def _ToTrainingDB(self,lastN = -1):
        sqls = []
        resultDF = None
        if lastN <0:
            resultDF = self.df
        else:
            resultDF = self.df[-lastN:]

        for _, row in resultDF.iterrows():
            date = row["日期"]
            tableName = "stockdailyinfo_traning"
            sql = f'''UPDATE `{tableName}` SET `V/MA10` = '{row["V/MA10"]}',`V/MA20` = '{row["V/MA20"]}',`V/MA30` = '{row["V/MA30"]}',`V/MA60` = '{row["V/MA60"]}',`V/MA90` = '{row["V/MA90"]}',`V/MA120` = '{row["V/MA120"]}',`V/MA250` = '{row["V/MA250"]}', `1日后涨幅` = '{row["1日后涨幅"]}', `3日后涨幅` = '{row["3日后涨幅"]}', `5日后涨幅` = '{row["5日后涨幅"]}', `7日后涨幅` = '{row["7日后涨幅"]}' WHERE (`日期` = '{date}') and (`股票代码` = '{self.stockID}');'''
            sqls.append(sql)
        

        step = 300
        groupedSql = [" ".join(sqls[i:i+step]) for i in range(0,len(sqls),step)]
        for sql in groupedSql:
            self.dbConnection.Execute(sql)

    def _ToDailyDB(self,lastN = -1):
        sqls = []
        resultDF = None
        if lastN <0:
            resultDF = self.df
        else:
            resultDF = self.df[-lastN:]

        for _, row in resultDF.iterrows():
            date = row["日期"]
            tableName = "stockdailyinfo"
            if date >="2021-01-01" and date <="2021-12-31":
                tableName = "stockdailyinfo_2021"

            if date >="2022-01-01" and date <="2022-12-31":
                tableName = "stockdailyinfo_2022"

            if date >="2023-01-01" and date <="2023-12-31":
                tableName = "stockdailyinfo_2023"

            sql = f'''UPDATE `{tableName}` SET `V/MA60` = '{row["V/MA60"]}', `1日后涨幅` = '{row["1日后涨幅"]}', `3日后涨幅` = '{row["3日后涨幅"]}', `5日后涨幅` = '{row["5日后涨幅"]}', `7日后涨幅` = '{row["7日后涨幅"]}' WHERE (`日期` = '{date}') and (`股票代码` = '{self.stockID}');'''
            sqls.append(sql)
    
        step = 10
        groupedSql = ["\n".join(sqls[i:i+step]) for i in range(0,len(sqls),step)]
        for sql in groupedSql:
            self.dbConnection.Execute(sql)

    def _UpdateVMA(self):
        
        self._setVMAN(10) #10 
        # self.df["V_MA10"] = self.df['成交量'].rolling(10).mean()
        # self.df["V/MA10"] = self.df.apply(lambda row: '{:.2f}'.format(row['成交量']/row["V_MA10"]), axis=1) 

        self._setVMAN(20) #20
        self._setVMAN(30) #30
        self._setVMAN(60) #60
        self._setVMAN(90) #90
        self._setVMAN(120)#120
        self._setVMAN(250)#250

        self.df["第2天开盘价"] = self.df['开盘价'].shift(-1)
        self.df["第3天开盘价"] = self.df['开盘价'].shift(-2)
        self.df["第5天开盘价"] = self.df['开盘价'].shift(-4)
        self.df["第7天开盘价"] = self.df['开盘价'].shift(-6)
        self.df["第9天开盘价"] = self.df['开盘价'].shift(-8)
        self.df["1日后涨幅"] = self.df.apply(lambda row: '{:.2f}'.format((row['第3天开盘价'] - row["第2天开盘价"])/row["第2天开盘价"]*100) , axis=1) 
        self.df["3日后涨幅"] = self.df.apply(lambda row: '{:.2f}'.format((row['第5天开盘价'] - row["第2天开盘价"])/row["第2天开盘价"]*100), axis=1) 
        self.df["5日后涨幅"] = self.df.apply(lambda row: '{:.2f}'.format((row['第7天开盘价'] - row["第2天开盘价"])/row["第2天开盘价"]*100), axis=1) 
        self.df["7日后涨幅"] = self.df.apply(lambda row: '{:.2f}'.format((row['第9天开盘价'] - row["第2天开盘价"])/row["第2天开盘价"]*100), axis=1) 
    
    def UpdateTrainingDataVMA(self,lastN = -1):
        self.GetTraingData()
        self._UpdateVMA()
        self._ToTrainingDB(lastN)

    def UpdateDailyDataVMA(self,lastN = -1):
        self.GetDailyData()
        self._UpdateVMA()
        self._ToDailyDB(lastN)