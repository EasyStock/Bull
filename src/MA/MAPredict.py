import pandas as pd
import numpy as np

class CMAPredict(object):
    def __init__(self,N,data) -> None:
        self.N = N
        self.data = data
        self.key = f'MA{self.N}'
        self.key0 = "数据"
        self.key1 = f'乖离_MA{self.N}'
        self.key2 = f'乖离率_MA{self.N}(%)'
        self.key3 = "昨日数据"
        self.key4 = "涨幅"
        self.yesterdayKey = f'MA{self.N}_昨日'
        self.deltaKey = f'MA{self.N}_Delta'
        self.dataFrame = None
        self.dataFrame = self._buildDataFrame(self.data)
    
    def _buildDataFrame(self,data):
        dataFrame = pd.DataFrame(list(data),columns=(self.key0,))
        dataFrame[self.key3] = dataFrame[self.key0].shift()
        dataFrame[self.key4] = (dataFrame[self.key0] - dataFrame[self.key3]) / dataFrame[self.key3]*100
        dataFrame[self.key] = dataFrame[self.key0].rolling(window=self.N).mean()
        dataFrame[self.yesterdayKey] = dataFrame[self.key].shift()
        dataFrame[self.deltaKey] = dataFrame[self.key] -  dataFrame[self.yesterdayKey]
        dataFrame[self.key1] = dataFrame[self.key0] - dataFrame[self.key] 
        dataFrame[self.key2] = (dataFrame[self.key0] - dataFrame[self.key]) / dataFrame[self.key]*100
        return dataFrame

    def _buildPredictData(self,data,steps):
        # X*(1+percentage) + MA5 + MA4 + MA3 + MA2 = 5X
        dataFrame = pd.DataFrame(list(data),columns=(self.key0,))
        MAkey = f'''MA{self.N -1}'''
        dataFrame[MAkey] = dataFrame[self.key0].rolling(window=self.N-1).sum()
        lastRow = dataFrame.iloc[-1]
        result = []
        for percentage in steps:
            res = list(data)
            x = lastRow[MAkey] / (self.N -1 - percentage/100)
            v = x*(1+percentage/100)
            res.append(v)
            d = {}
            d["预测乖离率"] = percentage 
            d["预测值"] = v
            d[f'''预测MA{self.N}'''] = x
            result.append(d)
        newDF = pd.DataFrame(result)
        return newDF
    
    def Predict(self,steps = list(np.linspace(-20, 20, 81))):
        return self._buildPredictData(self.data,steps)



if __name__ == "__main__":
    file = "/Users/mac/Desktop/正丹股份.csv"
    df = pd.read_csv(file)
    size = df.shape[0]
    result = []
    for i in range(6,size):
        newDF = df[:i]
        ma = CMAPredict(5,newDF["收盘价"])
        f = ma.Predict()
        newRow = df.iloc[i]
        open = newRow["开盘价"]
        a = f[f["预测值"] <= open]
        resultRow = newRow.copy()
        if not a.empty:
            resultRow["今日开盘价乖离率"] = a.iloc[-1]["预测乖离率"]
            if a.shape[0] == f.shape[0]:
                resultRow["今日开盘价乖离率"] = f'''>{a.iloc[-1]["预测乖离率"]}''' 
        else:
            a = f[f["预测值"] > open]
            resultRow["今日开盘价乖离率"] = f'''< {a.iloc[0]["预测乖离率"]}'''
        
        result.append(resultRow)
        print(resultRow)

    df = pd.DataFrame(result)
    print(df)
    df.to_excel("/tmp/aaaaaa.xlsx")
