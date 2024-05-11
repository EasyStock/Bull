import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
 

def FirstVolumnAlarm(df,liangbis = [1,3,5],threshold:dict ={"1日量比":2,"3日量比":1.5,"5日量比":2}):
    newDF = FilterByVolumn(df,liangbis,threshold)
    if not newDF.empty:
        return (False,None)
    else:
        last = df.iloc[-1]["日期"]
        last1 = newDF.iloc[-1]["日期"]
        res = list(newDF["日期"])
        if last == last1:
            return (True,None)
        
        return (False,res)

def FilterByVolumn(df,liangbis = [1,3,5],threshold:dict ={"1日量比":2,"3日量比":1.5,"5日量比":2}):
    newDF = pd.DataFrame(df,columns=["日期","收盘价","成交量"])
    newDF["序号"] = newDF.index + 1
    newDF["收盘价"] = newDF["收盘价"].astype("float")
    newDF["成交量"] = newDF["成交量"].astype("float")

    newDF["昨日收盘价"] =  newDF["收盘价"].shift()
    newDF["涨跌幅"] =  (newDF["收盘价"] - newDF["昨日收盘价"])/newDF["昨日收盘价"]*100

    newDF["昨日成交量"] =  newDF["成交量"].shift()

    for liangbi in liangbis:
        key = f'''{liangbi}日均量'''
        key1 = f'''{liangbi}日均量_昨日'''
        key2 = f'''{liangbi}日量比'''
        newDF[key] =  newDF["成交量"].rolling(window=liangbi).mean()
        newDF[key1] =  newDF[key].shift()
        newDF[key2] = newDF["成交量"]/newDF[key1]
        
    for key in threshold:
        value = threshold[key]
        newDF = newDF[newDF[key] >= value]
    
    if not newDF.empty:
        newDF.reset_index(inplace=True,drop=True)
        index0 = newDF.iloc[0]["序号"]
        newDF["第一次放量间隔"] = newDF["序号"] - index0

    return newDF


if __name__ == "__main__":
    file = "/Users/mac/Desktop/正丹股份.csv"
    df = pd.read_csv(file)
    newDF = FilterByVolumn(df)
    print(newDF)
    newDF.to_excel("/tmp/22.xlsx")


# # # 生成模拟数据
# # data = np.random.randn(1000)
 
# # 创建直方图
# plt.hist(df["成交量"], bins=50, color='green', edgecolor='black')
 
# # 设置标题和轴标签
# plt.title('Histogram of Data')
# plt.xlabel('Value')
# plt.ylabel('Frequency')
 
# # 显示图形
# plt.show()


