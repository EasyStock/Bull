import pandas as pd
import numpy as np

class CMA(object):
    def __init__(self,N,data:pd.DataFrame,column) -> None:
        self.N = N
        self.data = data
        self.column = column
        self.maDf = None
        self.key = f'MA{self.N}'
        self.yesterdayKey = f'MA{self.N}_昨日'
        self.deltaKey = f'MA{self.N}_Delta'
    
    def isUp(self):
        #是否拐头向上
        self._calcMA()

        if self.maDf.shape[0] <= self.N + 5:
            return False
        lastRow1 = self.maDf.iloc[-1]
        lastRow2 = self.maDf.iloc[-2]
        if lastRow1[self.deltaKey] > 0 and lastRow2[self.deltaKey]<=0:
            return True
        
        return False
    
    def isDown(self):
        #是否拐头向下
        self._calcMA()

        if self.maDf.shape[0] <= self.N + 5:
            return False
        
        lastRow1 = self.maDf.iloc[-1]
        lastRow2 = self.maDf.iloc[-2]
        if lastRow1[self.deltaKey] < 0 and lastRow2[self.deltaKey]>=0:
            return True
        
        return False

    def _calcMA(self):
        self.maDf = self.data.copy()
        self.maDf[self.key] = self.maDf[self.column].rolling(window=self.N).mean()
        self.maDf[self.yesterdayKey] = self.maDf[self.key].shift()
        self.maDf[self.deltaKey] = self.maDf[self.key] -  self.maDf[self.yesterdayKey]

    def keyMA(self):
        return self.data.iloc[-self.N-1][self.column]

