
import pandas as pd
import os
from workspace import workSpaceRoot
def DataFrameToSqls_INSERT_OR_IGNORE(datas,tableName):
    sqls = []
    for _, row in datas.iterrows():
        index_str = '''`,`'''.join(row.index)
        value_str = '''","'''.join(str(x) for x in row.values)
        sql = '''INSERT IGNORE INTO {0} (`{1}`) VALUES ("{2}");'''.format(tableName,index_str,value_str)
        sqls.append(sql)
    return sqls


class CYizhiban(object):
    def __init__(self,dbConnection,today) -> None:
        self.date = today
        self.dbConnection = dbConnection
        self.yiziban = None


    def GetYizhiban(self):
        if self.date is None:
            return

        sql = f'''SELECT A.`日期`,A.`股票代码`, B.`股票简称`,B.`涨停原因类别`, B.`连续涨停天数` FROM stock.stockdailyinfo As A, stock.stockzhangting AS B where A.`股票代码` = B.`股票代码` and A.`日期` = "{self.date}" and B.`日期` = "{self.date}" and A.`收盘价`=A.`开盘价` and A.`收盘价`=A.`最高价` and A.`最高价` = A.`最低价` and A.`涨跌幅` >0 and B.`股票简称` not like "%ST%" order by B.`连续涨停天数` DESC,B.`股票简称` DESC;'''
        results, columns = self.dbConnection.Query(sql)
        self.yiziban = pd.DataFrame(results,columns=columns)
        self.yiziban["封单915"] = ""
        self.yiziban["封单920"] = ""
        self.yiziban["封单925"] = ""
        self.yiziban["第二天表现"] = ""
        self.yiziban["第三天表现"] = ""
        print(self.yiziban)
        return self.yiziban


    def WriteToDB(self,tableName = "`stock`.`yiziban`"):
        if self.dbConnection is None or self.yiziban is None:
            return

        sqls = DataFrameToSqls_INSERT_OR_IGNORE(self.yiziban, tableName)
        for sql in sqls:
            self.dbConnection.Execute(sql)
            #print(sql)

    def WriteToLocalFile(self,path = f'''{workSpaceRoot}/复盘/股票/'''):
        if self.yiziban is None:
            return

        subPath = f'''{path}/{self.date}/'''
        if os.path.exists(subPath) == False:
            os.makedirs(subPath)

        fileName = f'''一字板_{self.date}.xlsx'''
        full = os.path.join(subPath,fileName)
        self.yiziban.to_excel(full)


    def YiZhiBan(self):
        print("CYizhiban.YiZhiBan begin.")
        self.GetYizhiban()
        self.WriteToDB()
        self.WriteToLocalFile()
        print("CYizhiban.YiZhiBan end.")
