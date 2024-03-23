import os
import pandas as pd
import numpy as np



class CPercentileBanKuai(object):
    def __init__(self,dbConnection):
        self.dbConnection = dbConnection

    def _formatVolumn(self,volumn:str):
        newVolumn = float(volumn[:-1])
        danwei = volumn[-1]
        ret = volumn
        if danwei == "万":
            ret = newVolumn*10000
        if danwei == "亿":
            ret = newVolumn*100000000
        return ret
    
    def _readExceptData(self):
        folder = os.path.abspath(os.path.dirname(__file__))
        banKuaifile = os.path.join(folder,"bankuai_except.xlsx")
        df_except = pd.read_excel(banKuaifile)
        return df_except

    def _DataFrameToSqls_REPLACE_INTO(self,datas,tableName):
        sqls = []
        for _, row in datas.iterrows():
            index_str = '''`,`'''.join(row.index)
            value_str = '''","'''.join(str(x) for x in row.values)
            sql = '''REPLACE INTO {0} (`{1}`) VALUES ("{2}");'''.format(tableName,index_str,value_str)
            sqls.append(sql)
        return sqls

    def PercentileBanKuai(self):
        sql = f'''SELECT * FROM stock.bankuai_index_dailyinfo;'''
        result1,columns1 = self.dbConnection.Query(sql)
        df_all=pd.DataFrame(result1,columns=columns1)

        df_except = self._readExceptData()
        newDF = df_all[~df_all.板块代码.isin(df_except.板块代码)]

        groups = newDF.groupby(["板块代码","板块名称"])

        step = list(np.linspace(0.01, 1, 100))
        for stockID, group in groups:
            if group.shape[0] < 10:
                continue
            group["成交量(股)"] = group.apply(lambda row:formatVolumn(row["成交量(股)"]),axis=1)
            group["成交额(元)"] = group.apply(lambda row:formatVolumn(row["成交额(元)"]),axis=1)
            group["流通市值(元)"] = group.apply(lambda row:formatVolumn(row["流通市值(元)"]),axis=1)
            group["总市值(元)"] = group.apply(lambda row:formatVolumn(row["总市值(元)"]),axis=1)
            group.reset_index(inplace=True)
            columns = ["开盘价(点)","收盘价(点)","最高价(点)","最低价(点)","成交量(股)","成交额(元)","涨跌幅(%)","量比","换手率(%)","上涨家数(家)","下跌家数(家)","流通市值(元)","总市值(元)"]
            newDf = pd.DataFrame(group, columns=columns)
            for c in columns:
                newDf[c] = newDf[c].astype(float)
                
            percentile = newDf.quantile(step)
            percentile["百分位数"] = percentile.index
            percentile["板块代码"] = stockID[0]
            percentile["板块名称"] = stockID[1]

            for c in columns:
                percentile[c] = percentile.apply(lambda row: '{:.2f}'.format(row[c]), axis=1)

            percentile["百分位数"] = percentile.apply(lambda row: '{:.02f}'.format(row["百分位数"]), axis=1)
            percentile.reset_index(inplace=True,drop=True)

            #t.set_index(["板块代码","百分位数"],drop=True,inplace=True)
            sqls = self._DataFrameToSqls_REPLACE_INTO(percentile,"percentile_bankuai")
            sql2 = " ".join(sqls)
            self.dbConnection.Execute(sql2)



def _GetAllBanKuaiInfo(dbconnection):
    sql = f'''SELECT `板块代码`,`板块名称` FROM stock.bankuai_index_dailyinfo group by `板块代码`,`板块名称`;'''
    result1,columns1 = dbconnection.Query(sql)
    df=pd.DataFrame(result1,columns=columns1)
    return df

def _GetAllValueableBanKuaiData(dbconnection,otherDf):
    folder = os.path.abspath(os.path.dirname(__file__))
    banKuaifile = os.path.join(folder,"bankuai_except.xlsx")
    df_except = pd.read_excel(banKuaifile)
    res = otherDf[~otherDf.板块代码.isin(df_except.板块代码)]
    return res

    # print(res.shape)
    # res.to_excel("/tmp/bankuai_except.xlsx",index=False)

def _GetAllBanKuaiData(dbConnection):
    sql = f'''SELECT * FROM stock.bankuai_index_dailyinfo;'''
    result1,columns1 = dbConnection.Query(sql)
    df=pd.DataFrame(result1,columns=columns1)
    return df


def GetAllValueableBanKuaiData(dbConnection):
    df = _GetAllBanKuaiData(dbConnection)
    res = _GetAllValueableBanKuaiData(dbConnection,df)
    print(res)

def GetAllBasicBanKuaiData(dbConnection):
    df = _GetAllBanKuaiInfo(dbConnection)
    res = _GetAllValueableBanKuaiData(dbConnection,df)
    res.to_excel("/tmp/_bankuai_except_.xlsx",index=False)
    return res


def formatVolumn(volumn:str):
    newVolumn = float(volumn[:-1])
    danwei = volumn[-1]
    ret = volumn
    if danwei == "万":
        ret = newVolumn*10000
    if danwei == "亿":
        ret = newVolumn*100000000
    return ret
    
def AnalysisBanKuai(dbConnection):
    df = _GetAllBanKuaiData(dbConnection)
    res = _GetAllValueableBanKuaiData(dbConnection,df)
    groups = res.groupby(["板块代码","板块名称"])
    results = []
    for stockID, group in groups:
        if group.shape[0] < 10:
            continue
        group["成交量(股)"] = group.apply(lambda row:formatVolumn(row["成交量(股)"]),axis=1)
        group["成交额(元)"] = group.apply(lambda row:formatVolumn(row["成交额(元)"]),axis=1)
        group["流通市值(元)"] = group.apply(lambda row:formatVolumn(row["流通市值(元)"]),axis=1)
        group["总市值(元)"] = group.apply(lambda row:formatVolumn(row["总市值(元)"]),axis=1)
        group.reset_index(inplace=True)
        columns = ["开盘价(点)","收盘价(点)","最高价(点)","最低价(点)","成交量(股)","成交额(元)","涨跌幅(%)","量比","换手率(%)","上涨家数(家)","下跌家数(家)","流通市值(元)","总市值(元)"]
        newDf = pd.DataFrame(group, columns=columns)
        for c in columns:
            newDf[c] = newDf[c].astype(float)
        print(group)
        step = list(np.linspace(0, 1, 101))
        t = newDf.quantile(step)

        print(type(t))