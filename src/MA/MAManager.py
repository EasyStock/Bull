from MA.MA1 import EventLast_MA1,Predict_MA1
from MA.MA2 import EventLast_MA2,Predict_MA2
from MA.MACross import CMACross

import pandas as pd


class CMAManager(object):
    def __init__(self,dbConnection):
        self.dbConnection = dbConnection
        self.messageToday = []
        self.messageTomorrow = []

    def _getIndexInfo(self):
        sql = f'''SELECT * FROM stock.kaipanla_index;'''
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)
        return df

    def _getIndexInfo2(self):
        file = "/Users/mac/Desktop/指数.csv"
        df = pd.read_csv(file)
        df.set_index("date",drop=True,inplace=True)
        return df

    def _getStockInfo(self,stockIDs:list):
        pass

    def _indexInfo_(self,df,stockID):
        newDf = df[df["StockID"] == stockID]
        res1 = EventLast_MA1(newDf["last_px"],MAs=[5,10,20,30,60,120,200])
        for key in res1:
            msg = f'''{key}, {",".join(res1[key])}'''
            self.messageToday.append(msg)
        res3 = EventLast_MA2(newDf["last_px"],MAs=[5,10,20,30,60,120,200])
        for key in res3:
            msg = f'''{key}, {",".join(res3[key])}'''
            self.messageToday.append(msg)

        res2 = Predict_MA1(newDf["last_px"],MAs=[5,10,20,30,60,120,200])
        self.messageTomorrow.extend(res2)
        res4 = Predict_MA2(newDf["last_px"],MAs=[5,10,20,30,60,120,200])
        self.messageTomorrow.extend(res4)

    def _indexInfo2_(self,df,stockID):
        newDf = df[df["StockID"] == stockID]
        MAs=[5,10,20,30,60,120,200]
        for i in range(len(MAs)):
            for j in range(i+1,len(MAs)):
                N1 = MAs[i]
                N2 = MAs[j]
                cross = CMACross(newDf,"last_px",N1,N2)
                isCrossUp = cross.isCrossUp()
                message = None
                if isCrossUp:
                    message = f''''MA{N1} 上穿 MA{N2}'''
                    self.messageToday.append(message)
                
                isCrossDown = cross.isCrossDown()
                if isCrossDown:
                    message = f''''MA{N1} 下穿 MA{N2}'''
                    self.messageToday.append(message)

                r = cross.predict()
                if r[1] >=-4 and r[1] <=4: #指数涨跌幅在【-4， 4 】之间
                    message = f'''预测: 数据为 {r[0]:.2f} 涨跌幅为:{r[1]:.2f}%时, MA{N1} 与 MA{N2}将在{r[2]:.2f}点 相交！'''
                    self.messageTomorrow.append((r[0],r[1],message))

    def _indexInfo(self,df,stockID,stockName):
        self._indexInfo_(df,stockID)
        self._indexInfo2_(df,stockID)
        msg1 = "\n  ".join(self.messageToday)
        sortedMessage = sorted(self.messageTomorrow,key = lambda x:x[0],reverse = True)
        messageTomorrow = ""
        for msg in sortedMessage:
            color = 'blue'
            if msg[1] > 0:
                color = 'red'
            else:
                color = 'green'
            newMsg = f'''**<font color='{color}'>{msg[0]:.2f}</font>** **<font color='blue'>{msg[2]}</font>**\n\n'''
            # data1 = f'''  {msg[0]:.2f}'''
            # data2 = msg[1]
            # newMsg = {"tag":"column_set","flex_mode":"none","background_style":"default","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":data1,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":data2,"text_align":"center"}]}],"horizontal_spacing":"small"}
            messageTomorrow = messageTomorrow + newMsg

        message = f'''
**<font color='red'>{stockName}【{stockID}】</font>** **今日信息:**
  {msg1}

**<font color='red'>{stockName}【{stockID}】</font>** **明日信息:**
{messageTomorrow} 
        '''
        #print(message)
        self.messageToday = []
        self.messageTomorrow = []
        return message

    def IndexInfo(self,):
        messages = []
        df = self._getIndexInfo()
        indexInfo = {
            "SH000001":"上证指数",
            "SZ399001":"深证成指",
            "SZ399006":"创业板指",
        }
        for stockID in indexInfo:
            stockName = indexInfo[stockID]
            res = self._indexInfo(df,stockID,stockName)
            messages.append(res)
        
        return messages

