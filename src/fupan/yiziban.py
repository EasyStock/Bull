
import pandas as pd
import os
from workspace import workSpaceRoot,WorkSpaceFont,GetStockFolder

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

    def WriteToLocalFile(self,path):
        if self.yiziban is None:
            return
        
        fileName = f'''一字板_{self.date}.xlsx'''
        full = os.path.join(path,fileName)
        self.yiziban.to_excel(full)

        JPGfileName = f'''一字板_{self.date}.jpg'''
        JPGfull = os.path.join(path,JPGfileName)
        df = pd.DataFrame(self.yiziban,columns=("股票代码","股票简称"))
        self.ConvertDataFrameToJPG(df,JPGfull)


    def ConvertDataFrameToJPG(self,df,fullPath):
        if df.empty:
            return
        from pandas.plotting import table
        import matplotlib.pyplot as plt
        plt.rcParams["font.sans-serif"] = [WorkSpaceFont]#显示中文字体
        high = int(0.174 * df.shape[0]+0.5) +1
        fig = plt.figure(figsize=(3, high), dpi=400)#dpi表示清晰度
        ax = fig.add_subplot(111, frame_on=False) 
        ax.xaxis.set_visible(False)  # hide the x axis
        ax.yaxis.set_visible(False)  # hide the y axis
        table(ax, df, loc='center')  # 将df换成需要保存的dataframe即可
        plt.savefig(fullPath)
        plt.close()

    def YiZhiBan(self):
        print("CYizhiban.YiZhiBan begin.")
        self.GetYizhiban()
        self.WriteToDB()
        path = GetStockFolder(self.date)
        self.WriteToLocalFile(path)
        print("CYizhiban.YiZhiBan end.")
