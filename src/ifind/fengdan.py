import requests
import json
import pandas as pd

class CFengdan(object):
    def __init__(self,dbConnection,token) -> None:
        self.dbConnection = dbConnection
        self.token = token

    def formatVolumn(self,volumn,delta = 1.0):
        newVolumn = float(volumn) * delta
        s = newVolumn /100000000.0 # 除以1亿
        ret = "亿"
        if s <1:
            t = newVolumn / 10000.0
            ret = f'''{t:.0f}万'''
        else:
            ret = f'''{s:.2f}亿'''
        return ret
    
    def _formatSql(self,df,stockID,date):
        #fengdan915 = df.loc[f'''{date} 09:15:00''']
        index = list(df.index)
        last = None
        for t in index:
            time = t.split(" ")[1]
            if time > "09:20:00":
                break
            if  time> "09:19:50":
                last = t
            
        fengdan915 = df.iloc[0]
        if pd.isna(fengdan915["bid1"]):
            fengdan915 = df.iloc[1]
        fengdan920 = df.loc[last]
        fengdan925 = df.iloc[-1]
        v15 = self.formatVolumn(float(fengdan915["bidSize2"]) * float(fengdan915["bid1"]))
        v20 = self.formatVolumn(float(fengdan920["bidSize2"]) * float(fengdan920["bid1"]))
        if fengdan925["bidSize1"] > fengdan925["bidSize2"]:
            v25 = self.formatVolumn(float(fengdan925["bidSize1"]) * float(fengdan925["bid1"]))
        else:
            v25 = self.formatVolumn(float(fengdan925["bidSize2"]) * float(fengdan925["bid1"]))
        sql = f'''UPDATE `stock`.`yiziban` SET `封单915` = '{v15}', `封单920` = '{v20}', `封单925` = '{v25}' WHERE (`日期` = '{date}') and (`股票代码` = '{stockID}');'''
        return sql

        

    def ProcessOneRow(self,row,date):
        stockID = row["thscode"]
        time = row["time"]
        bidSize1 = row["table"]["bidSize1"]
        askSize1 = row["table"]["askSize1"]
        bidSize2 = row["table"]["bidSize2"]
        askSize2 = row["table"]["askSize2"]
        bid1 = row["table"]["bid1"]
        table = {
            "time":time,
            "bid1":bid1,
            "bidSize1":bidSize1,
            "askSize1":askSize1,
            "bidSize2":bidSize2,
            "askSize2":askSize2,
        }
        
        df = pd.DataFrame(table)
        df.set_index(["time",],drop=True,inplace=True)
        #print(df)
        sql = self._formatSql(df,stockID,date)
        print(sql)
        self.dbConnection.Execute(sql)


    def Query(self,stockIDs,date):
        codes = stockIDs[0]
        if len(stockIDs) >= 2:
            codes = ",".join(stockIDs)

        thsUrl = 'https://ft.10jqka.com.cn/api/v1/snap_shot'
        thsHeaders = {"Content-Type":"application/json","access_token":self.token} 
        thsPara = {"codes":codes,"indicators":"bid1,bidSize1,askSize1,bidSize2,askSize2","starttime":f"{date} 09:15:00","endtime":f"{date} 09:25:00"} 
        thsResponse = requests.post(url=thsUrl,json=thsPara,headers=thsHeaders)
        #print(thsResponse.content)
        items = json.loads(thsResponse.content)['tables']
        #print(items)
        for item in items:
            self.ProcessOneRow(item,date)


    def ProcessFengDanLastN(self,tradingDays):
        firstday = tradingDays[0]
        sql = f'''SELECT `日期`,`股票代码` FROM stock.yiziban where `日期` >= "{firstday}";'''
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)
        #df.set_index(["日期",],drop=True,inplace=True)

        for day in tradingDays:
            print("start to Process FengDan:",day)
            stockIDs = list(df[df['日期'] == day]['股票代码'])
            if len(stockIDs) == 0:
                continue
            self.Query(stockIDs,day)
            input()