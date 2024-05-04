class CMACross(object):
    def __init__(self,df,column,N1,N2) -> None:
        self.df = df
        self.column = column
        self.N1 = N1
        self.N2 = N2
        self.key1 = f'''MA{self.N1}'''
        self.key2 = f'''MA{self.N2}'''
        self.key3 = f'''MA{self.N1} - MA{self.N2}'''
        self.key4 = f'''MA{self.N1} - MA{self.N2}_昨日'''

    def _PreProcess(self,delta = None):
        newDF = self.df.copy()
        lastRow = self.df.iloc[-1]
        if delta is not None:
            newRow = lastRow.copy()
            newRow[self.column] =  lastRow[self.column] * (100+delta)/100.0
            newDF = newDF._append(newRow, ignore_index=False)

        newDF[self.key1] = newDF[self.column].rolling(window=self.N1).mean()
        newDF[self.key2] = newDF[self.column].rolling(window=self.N2).mean()
        newDF[self.key3] = newDF[self.key1] - newDF[self.key2]

        return newDF
    
    def isCrossDown(self,delta = None):
        newDF = self._PreProcess(delta = delta)
        lastRow1 = newDF.iloc[-1]
        lastRow2 = newDF.iloc[-2]
        if lastRow1[self.key3] < 0 and lastRow2[self.key3]>=0:
            return True
        
        return False
    
    def isCrossUp(self,delta = None):
        newDF = self._PreProcess(delta=delta)
        lastRow1 = newDF.iloc[-1]
        lastRow2 = newDF.iloc[-2]
        if lastRow1[self.key3] > 0 and lastRow2[self.key3]<=0:
            return True
        
        return False
     
    def Cross(self,delta = None):
        return (self.isCrossUp(delta) or self.isCrossDown(delta))
    

    def predict(self,delta = None):
        newDF = self._PreProcess(delta=delta)
        n1 = newDF.iloc[-self.N1][self.column]
        ma1 = newDF.iloc[-1][self.key1]

        n2 = newDF.iloc[-self.N2][self.column]
        ma2 = newDF.iloc[-1][self.key2]

        x = (self.N2 * n1 - self.N1*n2 + self.N1*self.N2*ma2 - self.N1*self.N2*ma1)/(self.N2 - self.N1)
        last = newDF.iloc[-1][self.column]
        delta = (x- last)/last*100
        return (x,delta)

    def Verify(self,delta):
        newDF = self._PreProcess(delta = delta)
        print(newDF[-5:])


    def FindAllCrossUp(self):
        newDF = self._PreProcess()
        newDF[self.key4] = newDF[self.key3].shift()
        newDF["是否上穿"] = (newDF[self.key4] <=0) & (newDF[self.key3] >0)
        newDF["是否下穿"] = (newDF[self.key4] >=0) & (newDF[self.key3] <0)
        resultDf = newDF[(newDF["是否上穿"]) | newDF["是否下穿"]]
        resultDf.reset_index(drop=False,inplace=True)
        return resultDf
