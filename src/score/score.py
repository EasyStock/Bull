import pandas as pd
import matplotlib.pyplot as plt
import datetime
import time


def score(df:pd.Series,percentail:list,reversed=False):
    pass



def scoreV(dbConnection):
    sql = f'''select * FROM stock.kezhuanzhai_all where `日期` = "2023-12-12";'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    newDf = pd.DataFrame(df,columns=["成交额(万元)","总市值（亿元)","流通市值（亿元)","PB"])
    t = newDf.quantile([0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9])
    print(t)
    score(newDf['成交额(万元)'],t['成交额(万元)'],False)
    # plt.rcParams["font.sans-serif"]='Arial Unicode MS'
    # plt.rcParams['axes.unicode_minus']=False
    # #%config InlineBackend.figure_format='svg'
    # t = [i for i in range(50,300,5)]
    # df['成交额(万元)'].plot.kde()
    # # plt.hist(x=df.现价,bins=30,
    # #         color="steelblue",
    # #         edgecolor="black")

    # #添加x轴和y轴标签
    # plt.xlabel("年龄")
    # plt.ylabel("病例数")

    # #添加标题
    # plt.title("患者年龄分布")

    # #显示图形
    # plt.show()

