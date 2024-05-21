from MA.MA1 import EventLast_MA1,Predict_MA1
from MA.MA2 import EventLast_MA2,Predict_MA2
import pandas as pd


class CIndexMAMgr(object):
    def __init__(self,dbConnection):
        self.dbConnection = dbConnection

    
    def _getIndexInfo(self):
        sql = f'''SELECT * FROM stock.kaipanla_index;'''
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)
        return df

    def _getIndexInfo2(self):
        file = "/Users/mac/Desktop/指.csv"
        df = pd.read_csv(file)
        df.set_index("date",drop=True,inplace=True)
        return df

    def _indexInfo_(self,df,stockID,stockName):
        newDf = df[df["StockID"] == stockID]
        messageToday = []
        messageTomorrow = []
        res1 = EventLast_MA1(newDf["last_px"],MAs=[5,10,20,30,60,120,200])
        for key in res1:
            msg = f'''{key}, {",".join(res1[key])}'''
            messageToday.append(msg)
        res3 = EventLast_MA2(newDf["last_px"],MAs=[5,10,20,30,60,120,200])
        for key in res3:
            msg = f'''{key}, {",".join(res3[key])}'''
            messageToday.append(msg)

        res2 = Predict_MA1(newDf["last_px"],MAs=[5,10,20,30,60,120,200])
        messageTomorrow.extend([res[1] for res in res2])
        res4 = Predict_MA2(newDf["last_px"],MAs=[5,10,20,30,60,120,200])
        msg1 = "\n  ".join(messageToday)
        msg2 = "\n  ".join(messageTomorrow)
        messageTomorrow.extend([res[1] for res in res4])
        message = f'''
{stockName}【{stockID}】今日信息:
  {msg1}

{stockName}【{stockID}】明日信息:
  {msg2} 
        '''
        print(message)


    def IndexInfo(self):
        df = self._getIndexInfo()
        self._indexInfo_(df,"SH000001","上证指数")
        self._indexInfo_(df,"SZ399001","深证成指")
        self._indexInfo_(df,"SZ399006","创业板指")
