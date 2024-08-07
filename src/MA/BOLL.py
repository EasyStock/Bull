import pandas as pd
import numpy as np

class CBOLL(object):
    def __init__(self,data,N = 20,threshold = [2,]) -> None:
        self.data = data
        self.N = N
        self.key0 = "数据" 
        self.key1 = f'MA{self.N}'
        self.key2= f'标准差{self.N}'
        self.threshold = threshold
        self.events = []
        self.keys = []
    
    def _buildData(self,data,index = None):
        dataFrame = pd.DataFrame(list(data),columns=(self.key0,),index=index)
        dataFrame[self.key1] = dataFrame[self.key0].rolling(window=self.N).mean()
        dataFrame[self.key2] = dataFrame[self.key0].rolling(window=self.N).std()
        for threshold in self.threshold:
            key1 = f'''BOLL_UP_{threshold}'''
            key2 = f'''BOLL_DWON_{threshold}'''
            key3 = f'''BOLL_{threshold}_距离'''
            key4 = f'''BOLL_{threshold}_距离_昨日'''
            key5 = f'''BOLL_{threshold}_距离_Delta'''
            dataFrame[key1] = dataFrame[self.key1] + threshold* dataFrame[self.key2]
            dataFrame[key2] = dataFrame[self.key1] - threshold* dataFrame[self.key2]
            dataFrame[key3] = dataFrame[key1] - dataFrame[key2]
            dataFrame[key4] = dataFrame[key3].shift()
            dataFrame[key5] = dataFrame[key3] - dataFrame[key4]

            self.keys.extend([key1,key2])
        
        return dataFrame

    def _formatNumber(self,number):
        newNumber = float(f'''{number:.2f}''')
        return newNumber
    
    def _events(self,dataFrame):
        enent_1 = []
        if dataFrame.shape[0] >= 2:
            lastRow2 = dataFrame.iloc[-2]
            lastRow1 = dataFrame.iloc[-1]
            delta2 = lastRow2[self.key3]

            data = self._formatNumber(lastRow1[self.key0])
            delta1 = self._formatNumber(lastRow1[self.key3])

            if delta2 < 0 and delta1 >=0:
                enent_1 = [lastRow1.name,data,"拐头向上",self.key1,delta1]

            if delta2 > 0 and delta1 <= 0:
                enent_1 = [lastRow1.name,data,"拐头向下",self.key1,delta1]
        
        return enent_1

    def Predict(self):
        #预测明日均线拐头时数据的值
        dataFrame = self._buildData(self.data,self.data.index)
        keyRow = dataFrame.iloc[-self.N]
        lastRow1 = dataFrame.iloc[-1]
        data = self._formatNumber(keyRow[self.key0])
        zhangfu = (keyRow[self.key0] - lastRow1[self.key0]) / lastRow1[self.key0] * 100
        if lastRow1[self.key3] > 0:
            message = f'''预测: 数据小于等于{data} [{zhangfu:.2f}%], MA{self.N} 将拐头向下'''
        else:
            message = f'''预测: 数据大于等于{data} [{zhangfu:.2f}%], MA{self.N} 将拐头向上'''

        return (data,zhangfu,message)

    def EventLast(self):
        dataFrame = self._buildData(self.data,self.data.index)
        #print(dataFrame)
        dataFrame.to_excel("/tmp/aaa.xlsx")
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
        df.dropna(inplace=True)
        print(df)
        df.to_excel("/tmp/MA2.xlsx")
        return df

def Predict_MA2(data,MAs = [5,10,20,30,60,120,200]):
    result = []
    for ma in MAs:
        newMA = CBOLL(data,ma)
        re = newMA.Predict()
        result.append(re)
    
    sortedMA = sorted(result,key = lambda x:x[0],reverse = True)
    return sortedMA

def EventLast_BOLL(data,MAs = [5,10,20,30,60,120,200]):
    result = {}
    for ma in MAs:
        newMA = CBOLL(data,ma)
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
    df.set_index("日期",drop=True,inplace=True)


    # normal = CMA2(df["last_px"],5)
    # t = normal.Predict()
    # print(t)
    # #Predict(df["last_px"])
    # newDf = normal.EventLast()
    # print(newDf)
    # normal.EventEveryDay()

    print(EventLast_BOLL(df["收盘价(点)"],MAs=[20,]))
    # print(Predict_MA2(df["last_px"]))

