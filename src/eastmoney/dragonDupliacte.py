import pandas as pd


class CDragonDuplicate(object):
    def __init__(self,dbConnection,tableName,tableName_guanlian,tradingDay) -> None:
        self.dbConnection = dbConnection
        self.tradingDay = tradingDay
        self.tableName = tableName
        self.allData = None
        self.tableName_guanlian = tableName_guanlian
    

    def Analysis(self):
        pass


    def GetData(self):
        sql = f'''select * from {self.tableName}  where date = "{self.tradingDay}";'''
        print(sql)
        data, columns = self.dbConnection.Query(sql)
        self.allData = pd.DataFrame(data,columns=columns)
        #print(self.allData)
        self._groupByStockID()

    def _groupByOperatorID(self,stockID,flag, df):
        groupBy_operator_ID = df.groupby(["operator_ID",]).count()
        operator_IDs = list(groupBy_operator_ID.index)
        results = []
        for operator_ID in operator_IDs:
            dataFrame =df[df["operator_ID"] == operator_ID]
            reasons = "++++".join(list(dataFrame['reason']))
            operator_Name = list(set(dataFrame["operator_Name"]))[0]
            result = [stockID,operator_ID,operator_Name,flag,reasons]
            results.append(result)

        size = len(results)
        ret = []
        except_OperatorIDs = ["0","10472087","10428246","10484371","10495103",] # 去除了机构席位，拉萨席位
        for index in range(size):
            for index2 in range(index+1,size):
                k1 = results[index]
                k2 = results[index2]
                if k1[1] in except_OperatorIDs or k2[1] in except_OperatorIDs: # operator_ID 为0 的机构席位排除
                    continue
                
                r1 = None
                name = None
                if k1[1] > k2[1]:
                    r1 = f'''{k1[1]}_{k2[1]}'''
                    name = f'''{k1[2]}_{k2[2]}'''
                else:
                    r1 = f'''{k2[1]}_{k1[1]}'''
                    name = f'''{k2[2]}_{k1[2]}'''

                res = [r1,name,stockID,self.tradingDay,flag,k1[4]]
                ret.append(res)
        return ret


    def _DataFrameToSqls_INSERT_OR_IGNORE(self,datas,tableName):
        sqls = []
        for _, row in datas.iterrows():
            index_str = '''`,`'''.join(row.index)
            value_str = '''","'''.join(str(x) for x in row.values)
            sql = '''INSERT IGNORE INTO {0} (`{1}`) VALUES ("{2}");'''.format(tableName,index_str,value_str)
            sqls.append(sql)
        return sqls


    def _groupByStockID(self):
        if self.allData is None:
            return 

        groupByStockID = self.allData.groupby(["stockID",]).count()
        
        stockIDs = list(groupByStockID.index)
        columns = ["operatorIDs","operatorIDNames","stockID","date","flag","reason"]
        res = []
        for stockID in stockIDs:
            stockIDDataFrame =self.allData[self.allData["stockID"] == stockID]
            #print(stockIDDataFrame)
           
            flag_B = stockIDDataFrame[stockIDDataFrame["flag"] == "B"]
            flag_S = stockIDDataFrame[stockIDDataFrame["flag"] == "S"]
            # print(flag_B)
            # print(flag_S)
            res1 = self._groupByOperatorID(stockID,"B",flag_B)
            res2 = self._groupByOperatorID(stockID,"S",flag_S)
            res.extend(res1)
            res.extend(res2)
        
        df = pd.DataFrame(res,columns=columns)
        sqls = self._DataFrameToSqls_INSERT_OR_IGNORE(df,self.tableName_guanlian)
        for sql in sqls:
            self.dbConnection.Execute(sql)
            #print(sql)