import numpy as np
from MA.MA import CMA
from MA.MACross import CMACross

class CMAMgr(object):
    def __init__(self) -> None:
        pass

    def _predict(self,df,column,N):
        up = []
        down = []
        lastRow = df.iloc[-1]
        keyMa = None
        steps = list(np.linspace(-10, 10, 41))
        for delta in steps:
            newRow = lastRow.copy()
            newRow[column] =  lastRow[column] * (100+delta)/100.0
            newDf = df.copy()
            newDf = newDf._append(newRow, ignore_index=False)
            newMa = CMA(N,newDf,column)
            #print(newMa.data.iloc[-1]["last_px"],delta,newMa.keyMA())
            if keyMa is None:
                keyMa = newMa.keyMA()
            if newMa.isUp():
                up.append(delta)
            if newMa.isDown():
                down.append(delta)

        percentage = (keyMa - lastRow[column])/lastRow[column]*100
        result = []
        if len(up) >0:
            message = f'''今天是:{lastRow.name},明天点位高于【{keyMa},>={percentage:.2f}%】,{N}日线将 【拐头向上】'''
            print(message)
            result.append(message)

        if len(down) >0:
            message = f'''今天是:{lastRow.name},明天点位低于【{keyMa},<={percentage:.2f}%】,{N}日线将 【拐头向下】'''
            print(message)
            result.append(message)
        return result

    def predict(self,df,column,N):
        if isinstance(N,int):
            return self._predict(df,column,N)
        elif isinstance(N,(list,tuple)):
            res = []
            for n in N:
                r = self._predict(df,column,n)
                res.extend(r)
            return res

    def Cross(self,df,column,N1,N2):
        key1 = f'''MA{N1}'''
        key2 = f'''MA{N2}'''
        key3 = f'''MA{N1} - MA{N2}'''
        newDF = df.copy()
        newDF[key1] = newDF[column].rolling(window=N1).mean()
        newDF[key2] = newDF[column].rolling(window=N2).mean()
        newDF[key3] = newDF[key1] - newDF[key2]
        lastRow1 = newDF.iloc[-1]
        lastRow2 = newDF.iloc[-2]
        result = []
        if lastRow1[key3] < 0 and lastRow2[key3]>0:
            message = f'''今天是:{lastRow2.name} ,{N1}日线将 下穿 {N2} 日线'''
            print(message)
            result.append(message)
        elif lastRow1[key3] > 0 and lastRow2[key3]<0:
            message = f'''今天是:{lastRow2.name} ,{N1}日线将 上穿 {N2} 日线'''
            print(message)
            result.append(message)
        return result

        