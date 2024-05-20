import pandas as pd
import numpy as np

class CMA1(object):
    # 只是计算当日的MA情况，包括跌破，站上
    def __init__(self,data,N) -> None:
        self.data = data
        self.N = N
        self.key0 = "数据"
        self.key1 = f'MA{self.N}'
        self.key2= f'MA{self.N-1}'
        self.events = []
    
    def _buildData(self,data,index = None):
        dataFrame = pd.DataFrame(list(data),columns=(self.key0,),index=index)
        dataFrame[self.key1] = dataFrame[self.key0].rolling(window=self.N).mean()
        dataFrame[self.key2] = dataFrame[self.key0].rolling(window=self.N-1).mean()

        return dataFrame

    def _formatNumber(self,number):
        newNumber = float(f'''{number:.2f}''')
        return newNumber
    
    def _events(self,dataFrame):
        enent_1 = []
        if dataFrame.shape[0] >= 2:
            lastRow2 = dataFrame.iloc[-2]
            lastRow1 = dataFrame.iloc[-1]
            data_2 = lastRow2[self.key0]
            ma_2 = lastRow2[self.key1]

            data = self._formatNumber(lastRow1[self.key0])
            ma1 = self._formatNumber(lastRow1[self.key1])

            if data_2 < ma_2 and data >=ma1:
                enent_1 = [lastRow1.name,data,"站上",self.key1,""]

            if data_2 > ma_2 and data < ma1:
                enent_1 = [lastRow1.name,data,"跌破",self.key1,""]
        
        return enent_1

    def Predict(self):
        #预测明日的均线平衡点，即价格 = 均线
        dataFrame = self._buildData(self.data,self.data.index)
        lastRow1 = dataFrame.iloc[-1]
        ma = self._formatNumber(lastRow1[self.key2])
        message = f'''{ma} 是 MA{self.N} 等于价格的点'''
        return (ma,message)

    def EventLast(self):
        dataFrame = self._buildData(self.data,self.data.index)
        #print(dataFrame)
        self.events = self._events(dataFrame)
        #print(self.events)
        return self.events
    
    def EventEveryDay(self):
        dataFrame = self._buildData(self.data,self.data.index)
        size = dataFrame.shape[0]
        events = []
        for index in range(6,size+1):
            if index == size:
                newDf = dataFrame
            else:
                newDf = dataFrame[:index]
            evts1 = self._events(newDf)
            events.append(evts1)
        
        df = pd.DataFrame(events)
        return df

def Predict_MA1(data,MAs = [5,10,20,30,60,120,200]):
    result = []
    for ma in MAs:
        newMA = CMA1(data,ma)
        re = newMA.Predict()
        result.append(re)
    
    sortedMA = sorted(result,key = lambda x:x[0],reverse = True)
    return sortedMA

def EventLast_MA1(data,MAs = [5,10,20,30,60,120,200]):
    result = {}
    for ma in MAs:
        newMA = CMA1(data,ma)
        re = newMA.EventLast()
        if len(re)>=1:
            eventName = re[2]
            if eventName not in result:
                result[eventName] = []

            result[eventName].append(re[3])
    return result


if __name__ == "__main__":
    file = "/Users/mac/Desktop/上证指数.csv"
    df = pd.read_csv(file)
    df.set_index("date",drop=True,inplace=True)
    # normal = CMA1(df["last_px"],10)
    # t = normal.Predict()
    # print(t)
    # #Predict(df["last_px"])
    # newDf = normal.EventLast()

    print(EventLast_MA1(df["last_px"]))
    print(Predict_MA1(df["last_px"]))

