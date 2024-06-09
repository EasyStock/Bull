
import pandas as pd

def GetZhaiData(dbConnection):
    sql = f'''SELECT * FROM stock.kezhuanzai_ths;'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    return df


def _filter1(df):
    df["最高价"] = df["最高价"].astype("float")
    df["最低价"] = df["最低价"].astype("float") 
    df["开盘价"] = df["开盘价"].astype("float")
    df["收盘价"] = df["收盘价"].astype("float")
    df["成交量"] = df["成交量"].astype("float")

    df["昨日收盘价"] = df["收盘价"].shift()
    df["今日开盘价涨幅"] = (df["开盘价"] - df["昨日收盘价"]) / df["昨日收盘价"] * 100
    df["今日收盘价涨幅"] = (df["收盘价"] - df["昨日收盘价"]) / df["昨日收盘价"] * 100
    df["今日最高价涨幅"] = (df["最高价"] - df["昨日收盘价"]) / df["昨日收盘价"] * 100
    df["今日最低价涨幅"] = (df["最低价"] - df["昨日收盘价"]) / df["昨日收盘价"] * 100

    df["今日开盘-收盘价涨幅"] = df["今日开盘价涨幅"] - df["今日收盘价涨幅"]
    df["今日最高-开盘价涨幅"] = df["今日最高价涨幅"] - df["今日开盘价涨幅"]
    df["今日收盘-最低价涨幅"] = df["今日收盘价涨幅"] - df["今日最低价涨幅"]


    return df


def Filter1(df):
    df = _filter1(df)
    if df.iloc[-1]["今日开盘价涨幅"] > 3 and df["今日开盘-收盘价涨幅"] > 3:
        return True
    

def FitlerAll(df):
    df = _filter1(df)
    newDf = df[df["今日开盘价涨幅"] > 3]
    newDf = newDf[newDf["今日开盘-收盘价涨幅"] > 3]
    newDf = newDf[newDf["今日最高-开盘价涨幅"] <= 3]
    newDf = newDf[newDf["今日收盘-最低价涨幅"] <= 3]
    if not newDf.empty:
        print(newDf)
        return newDf
    
    return None


def Test(dbConnection):
    df = GetZhaiData(dbConnection)
    groups = df.groupby("转债代码")
    all = []
    for stockID, group in groups:
        res = FitlerAll(group)
        if res is not None:
            all.append(res)

    df = pd.concat(all)
    df.sort_values("日期",ascending=False,inplace=True)
    df.to_excel("/tmp/aaaaaa.xlsx")