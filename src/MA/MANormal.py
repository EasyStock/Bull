import pandas as pd
import numpy as np

class CMANormal(object):
    # 只是计算当日的MA情况，包括跌破，站上,乖离
    def __init__(self,data,N) -> None:
        self.data = data
        self.N = N
        self.key0 = "数据"
        self.key1 = f'MA{self.N}'
        self.key2 = f'MA{self.N}_昨日'
        self.key3 = f'MA{self.N-1}'
        self.key4 = f'MA{self.N-1}_昨日'
        self.key5 = f'乖离_MA{self.N}'
        self.key6 = f'乖离率_MA{self.N}(%)'
        self.key7 = f'''MA{self.N}乖离率分数'''
        self.events = []
    
    def _buildData(self,data,index = None):
        dataFrame = pd.DataFrame(list(data),columns=(self.key0,),index=index)

        dataFrame[self.key1] = dataFrame[self.key0].rolling(window=self.N).mean()
        dataFrame[self.key2] = dataFrame[self.key1].shift()

        dataFrame[self.key3] = dataFrame[self.key0].rolling(window=self.N-1).mean()
        dataFrame[self.key4] = dataFrame[self.key3].shift()


        dataFrame[self.key5] = dataFrame[self.key0] - dataFrame[self.key1] 
        dataFrame[self.key6] = (dataFrame[self.key0] - dataFrame[self.key1]) / dataFrame[self.key1]*100

        step = list(np.linspace(0, 1, 101))
        quantileDf = dataFrame[self.key6].quantile(step)
        dataFrame.dropna(inplace=True)
        dataFrame[self.key7] = dataFrame.apply(lambda row: self._score(row[self.key6],quantileDf,False), axis=1)

        return dataFrame
    
    def _score(self,volumn,percentail,reversed = False):
        result = 1
        for index, value in percentail.items():
            if float(volumn) <= float(value):
                result = float(index) - 0.5
                break
        
        if reversed:
            result = 0.5 - float(result)
        
        result = int(result * 100)
        return result*2
    
    def _formatNumber(self,number):
        newNumber = float(f'''{number:.2f}''')
        return newNumber
    
    def _events(self,dataFrame):
        enent = []
        if dataFrame.shape[0] >= 2:
            lastRow2 = dataFrame.iloc[-2]
            lastRow1 = dataFrame.iloc[-1]
            data_2 = lastRow2[self.key0]
            ma_2 = lastRow2[self.key1]
            man_1_2 = self._formatNumber(lastRow2[self.key3])

            data = self._formatNumber(lastRow1[self.key0])
            ma1 = self._formatNumber(lastRow1[self.key1])
            man_1 = self._formatNumber(lastRow1[self.key3])
            guali = self._formatNumber(lastRow1[self.key6])
            gualilv = self._formatNumber(lastRow1[self.key7])

            if data_2 < ma_2 and data >=ma1:
                res = [lastRow1.name,data,"站上",self.key1,man_1_2]
                enent.append(res)

            if data_2 > ma_2 and data < ma1:
                res = [lastRow1.name,data,"跌破",self.key1,man_1_2]
                enent.append(res) 

            enent.append([lastRow1.name,data,"明日均衡点",self.key1,man_1]) 
            enent.append([lastRow1.name,data,"乖离",guali,gualilv]) 
        return enent

    def EventLast(self):
        dataFrame = self._buildData(self.data,self.data.index)
        print(dataFrame)
        self.events = self._events(dataFrame)
        print(self.events)
    
    def EventEveryDay(self):
        dataFrame = self._buildData(self.data,self.data.index)
        size = dataFrame.shape[0]
        events = []
        for index in range(6,size+1):
            if index == size:
                newDf = dataFrame
            else:
                newDf = dataFrame[:index]
            evts = self._events(newDf)
            #lastRow = newDf.iloc[-1]
            events.extend(evts)
        #print(events)
        df = pd.DataFrame(events)
        print(df)


if __name__ == "__main__":
    file = "/Users/mac/Desktop/上证指数.csv"
    df = pd.read_csv(file)
    df.set_index("date",drop=True,inplace=True)
    normal = CMANormal(df["last_px"],5)
    normal.EventLast()