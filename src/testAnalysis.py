import pandas as pd




class CResult(object):
    def __init__(self,df,tag= None,shouyi = "3日后卖出收益"):
        self.df = df
        self.tag = tag
        self.shouyi = shouyi

        self.totalSize = 0
        self.size =  0
        self.successRatio = 0
        self.average = 0.0
        self.max = 0.0
        self.min = 0.0
        self.quantile25 = 0.0
        self.quantile50 = 0.0
        self.quantile75 = 0.0

    def _updateResult(self):
        self.result = {
            "标记":self.tag,
            "数据总量":self.totalSize,
            "大于0数量":self.size,
            "成功率":f'''{self.successRatio:.2f}''',
            "平均收益":f'''{self.average:.2f}''',
            "最大收益":f'''{self.max:.2f}''',
            "最小收益":f'''{self.min:.2f}''',
            "25百分位":f'''{self.quantile25:.2f}''',
            "50百分位":f'''{self.quantile50:.2f}''',
            "70百分位":f'''{self.quantile75:.2f}''',
        }
        return self.result
    

    def CalcResult(self):
        newDf = self.df[self.df[self.shouyi].notna()]
        self.totalSize = newDf.shape[0]

        if self.totalSize== 0:
            self._updateResult()
            return
        
        shouyi = self.df[self.df[self.shouyi]>=0]
        self.size = shouyi.shape[0]
        self.successRatio = self.size/self.totalSize * 100.0
        self.average = shouyi[self.shouyi].mean()
        self.max = shouyi[self.shouyi].max()
        self.min = shouyi[self.shouyi].min()
        self.quantile25 = shouyi[self.shouyi].quantile(0.25)
        self.quantile50 = shouyi[self.shouyi].quantile(0.5)
        self.quantile75 = shouyi[self.shouyi].quantile(0.75)
        
        self._updateResult()

    def __str__(self):
        message = f'''tag:{self.tag}     total: {self.totalSize:>05},     [>0] :{self.size:>05},       percentage :{self.successRatio:>8.02f}%,      平均收益 :{self.average:>5.02f}      最高收益: {self.max:>5.02f}     最低收益: {self.min:05.02f},     25%收益: {self.quantile25:05.02f},     50%收益: {self.quantile50:05.02f},      75%收益: {self.quantile75:05.02f}'''
        return message
    

def ReadFile():
    fileName = "/Users/mac/Desktop/BBB.xlsx"
    df = pd.read_excel(fileName)
    return df

def GetPercentage(df:pd.DataFrame,tag = None,shouyi = "1日后卖出收益"):
    res = CResult(df,tag,shouyi)
    res.CalcResult()
    print(res)
    return res.result

    # df = df[df[shouyi].notna()]
    # totalsize = df.shape[0]
    # if totalsize == 0:
    #     return
    # s = df[df[shouyi] >=0]
    # size = s.shape[0]
    # per = size/totalsize*100.0
    # avg = s[shouyi].mean()
    # high = s[shouyi].max()
    # low = s[shouyi].min()
    # # t= .describe()
    # message = f'''tag:{tag}     total: {totalsize:>5},     [>0] :{size:>5},       percentage :{per:>8.02f}%,      平均收益 :{avg:>5.02f}      最高收益: {high:>5.02f}     最低收益: {low:5.02f},     25%收益: {s[shouyi].quantile(0.25):5.02f},     50%收益: {s[shouyi].quantile(0.5):5.02f},      75%收益: {s[shouyi].quantile(0.75):5.02f}'''
    # print(message)

def Filter1(df):
    result = []
    for i in range(1,81):
        newDf = df[df["买入排名"] ==i]
        tag = f'''买入排名:{i:03}'''
        res = GetPercentage(newDf,tag = tag)
        result.append(res)
    df = pd.DataFrame(result)
    print(df)

def Filter2(df):
    for i in range(20,-20,-1):
        newDf = df[df["买入价格涨跌幅"] <=i] # 开盘价涨跌幅
        tag = f'''买入价格涨跌幅<={i:03}'''
        GetPercentage(newDf,tag = tag)

def Filter3(df):
    for i in range(-20,20,1):
        newDf = df[df["买入价格涨跌幅"] >=i] # 开盘价涨跌幅
        tag = f'''买入价格涨跌幅>={i:03}'''
        GetPercentage(newDf,tag = tag)

def Filter4(df):
    result = []
    for i in range(-20,20,1):
        newDf = df[df["涨跌幅"] >=i]  # 前一日涨跌幅
        tag = f'''当日涨跌幅>={i:03}'''
        GetPercentage(newDf,tag = tag)

def Filter5(df):
    result = []
    for i in range(20,-20,-1):
        newDf = df[df["涨跌幅"] <=i]   # 前一日涨跌幅
        tag = f'''当日涨跌幅<={i:03}'''
        res = GetPercentage(newDf,tag = tag)
        result.append(res)

    df = pd.DataFrame(result)
    df.to_excel("/tmp/Filter5.xlsx")

def Filter6(df):
    for i in range(0,15,1):
        newDf = df[df["V/MA10"] >= i]   # 前一日涨跌幅
        tag = f'''V/MA10 >={i:03}'''
        GetPercentage(newDf,tag = tag)

def Filter7(df):
    for i in range(0,15,1):
        newDf = df[df["V/MA10"] <= i]   # 前一日涨跌幅
        tag = f'''V/MA10 <={i:03}'''
        GetPercentage(newDf,tag = tag)


def Filter8(df):
    for i in range(0,15,1):
        newDf = df[df["V/MA20"] >= i]   # 前一日涨跌幅
        tag = f'''V/MA20 >={i:03}'''
        GetPercentage(newDf,tag = tag)

def Filter9(df):
    for i in range(0,15,1):
        newDf = df[df["V/MA20"] <= i]   # 前一日涨跌幅
        tag = f'''V/MA20 <={i:03}'''
        GetPercentage(newDf,tag = tag)



if __name__ == "__main__":
    df = ReadFile()
    df = df[df["名称"] == "自动选股_30_10_20_40"]
    GetPercentage(df)
    #Filter1(df)
    #Filter2(df)
    #Filter3(df)
    #Filter4(df)
    Filter5(df)
