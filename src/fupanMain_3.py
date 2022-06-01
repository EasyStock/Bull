
import string
from mysql.connect2DB import ConnectToDB
from DBOperating import GetTradingDateLastN,Get1LianBan,Get2LianBan,Get3LianBan,Get4AndMoreLianBan,GaoWeiFailed,\
                        Get10CMShouBanZhangTingData,Get20CMShouBanZhangTingData,Get10CMLianBanZhangTingData,\
                        Get20CMLianBanZhangTingData,ZhangTingFengdan,GetTodayMarketingData,DongNeng
from ColoredLog import StartToInitLogger
import pandas as pd
import schedule
import time
import datetime
import re
import requests
import json

class CFuPan(object):
    def __init__(self,logger, dbConnection,lastN=-1) -> None:
        super().__init__()
        self.shouBan10CMRatio = 0
        self.shouBan20CMRatio = 0
        self.lianBan10CMRatio = 0
        self.lianBan20CMRatio = 0
        self.lastN = lastN
        self.dbConnection = dbConnection
        self.tradingDays = GetTradingDateLastN(dbConnection,15)
        self.today = self.tradingDays[lastN]
        self.yestoday = self.tradingDays[lastN-1]
        self.logger = logger
        self.fuPanFullPath = f"/Volumes/Data/Downloads/网盘同步/BaiduNetdiskWorkspace/复盘记录/复盘记录表_{self.today}.xlsx"
        self.excelWriter = pd.ExcelWriter(self.fuPanFullPath)
        self.shiNeng = 0
        self.dongNeng = 0
        self.countOfZhangTing = 0
        self.countOfDieTing = 0
        self.gaoDuBan = 0
        self.lianbanCount = 0
        self.lianBan1 = []
        self.lianBan2 = []
        self.lianBan3 = []
        self.lianBan4 = []
        self.lianBan4AndMore = []
        self.hongPan = 0
        self.lvPan = 0
        
    def _ratio(self,dataFrame) -> float:
        shape = dataFrame.shape
        if shape[0] == 0:
            return -2
        dataFrame['涨跌幅'] = dataFrame['涨跌幅'].astype(float)
        newDF = dataFrame[dataFrame['涨跌幅']>=0]
        shape2= newDF.shape
        return shape2[0]*1.0/shape[0]
        
    def Calc10CMShouBan(self) -> float:
        result = Get10CMShouBanZhangTingData(self.dbConnection,self.yestoday,self.today)
        self.logger.info(result[1])
        result[0].to_excel(self.excelWriter,"昨10CM首板",index=False)
        return self._ratio(result[0])
    
    def Calc20CMShouBan(self) -> float:
        result = Get20CMShouBanZhangTingData(self.dbConnection,self.yestoday,self.today)
        self.logger.info(result[1])
        result[0].to_excel(self.excelWriter,"昨20CM首板",index=False)
        return self._ratio(result[0])
    
    def Calc10CMLianBan(self) -> float:
        result = Get10CMLianBanZhangTingData(self.dbConnection,self.yestoday,self.today)
        self.logger.info(result[1])
        result[0].to_excel(self.excelWriter,"昨10CM连板",index=False)
        return self._ratio(result[0])
    
    def Calc20CMLianBan(self) -> float:
        result = Get20CMLianBanZhangTingData(self.dbConnection,self.yestoday,self.today)
        self.logger.info(result[1])
        result[0].to_excel(self.excelWriter,"昨20CM连板",index=False)
        return self._ratio(result[0])
    
    def FuPan(self):
        #首板奖励率
        self.shouBan10CMRatio = self.Calc10CMShouBan()*100
        self.shouBan20CMRatio = self.Calc20CMShouBan()*100
        #连板奖励率
        self.lianBan10CMRatio = self.Calc10CMLianBan()*100
        self.lianBan20CMRatio = self.Calc20CMLianBan()*100

        #动能
        self.dongNeng = DongNeng(self.dbConnection,self.yestoday,self.today)
        #今日首板
        res1 = Get1LianBan(self.dbConnection,self.today)
        self.logger.info(res1[1])

        res1[0].to_excel(self.excelWriter,"今日首板",index=False)
        if res1[0].shape[0] >0:
            self.gaoDuBan = 1
            self.shiNeng = self.shiNeng + res1[0]["连续涨停天数"].sum()
            self.lianBan1 = list(res1[0]["股票简称"])
        
        #今日2连板
        res2 = Get2LianBan(self.dbConnection,self.today)
        self.logger.info(res2[1])
        res2[0].to_excel(self.excelWriter,"今日2连板",index=False)
        if res2[0].shape[0] >0:
            self.gaoDuBan =2
            self.shiNeng = self.shiNeng + res2[0]["连续涨停天数"].sum()
            self.lianBan2 = list(res2[0]["股票简称"])
            self.lianbanCount = res2[0].shape[0]
            
        #今日3连板
        res3 = Get3LianBan(self.dbConnection,self.today)
        res3[0].to_excel(self.excelWriter,"今日3连板",index=False)
        self.logger.info(res3[1])
        if res3[0].shape[0] >0:
            self.gaoDuBan =3
            self.shiNeng = self.shiNeng + res3[0]["连续涨停天数"].sum()
            self.lianBan3 = list(res3[0]["股票简称"])
            self.lianbanCount = self.lianbanCount+ res3[0].shape[0]
             
        #今日4板及4板以上
        res4 = Get4AndMoreLianBan(self.dbConnection,self.today)
        res4[0].to_excel(self.excelWriter,"今日4连板及以上",index=False)
        self.logger.info(res4[1])
        if res4[0].shape[0] >0:
            self.gaoDuBan = res4[0]["连续涨停天数"].max()
            self.shiNeng = self.shiNeng + res4[0]["连续涨停天数"].sum()
            res4[0]["信息"] =  res4[0]["股票简称"].map(str)  + res4[0]["连续涨停天数"].map(str)
            self.lianBan4 = list(res4[0]["信息"])
            self.lianbanCount = self.lianbanCount+ res4[0].shape[0]
        
        #昨日3板及以上，今日断板
        res5 = GaoWeiFailed(self.dbConnection,self.yestoday,self.today)
        res5[0].to_excel(self.excelWriter,"今日高位断板",index=False)
        self.logger.info(res5[1])
        
        df = ZhangTingFengdan(self.dbConnection,self.today)
        df["9:15封单"]=""
        df["9:20封单"]=""
        df["9:25封单"]=""
        df.to_excel(self.excelWriter,"涨停封单",index=False)
        self.excelWriter.save()
    
    def FormatFuPanSqlAndToDB(self):
        row = ("日期","红盘","绿盘","实际涨停","跌停","连板","10CM首板奖励率","20CM首板奖励率","10CM连板奖励率","20CM连板奖励率","首板个数","2连板个数","3连板个数","3连个股","4连板及以上个数","4连及以上个股","高度板","动能","势能")
        values = (self.today, 
                  self.hongPan,
                  self.lvPan,
                  self.countOfZhangTing,
                  self.countOfDieTing,
                  self.lianbanCount,
                  f"{self.shouBan10CMRatio:.2f}",
                  f"{self.shouBan20CMRatio:.2f}",
                  f"{self.lianBan10CMRatio:.2f}",
                  f"{self.lianBan20CMRatio:.2f}",
                  len(self.lianBan1),
                  len(self.lianBan2),
                  len(self.lianBan3),
                  ";".join(self.lianBan3),
                  len(self.lianBan4),
                  ";".join(self.lianBan4),
                  self.gaoDuBan,
                  f"{self.dongNeng:.2f}",
                  self.shiNeng,
                  )
        index_str = '''`,`'''.join(row)
        value_str = '''","'''.join(str(x) for x in values)
        sql = '''REPLACE INTO `{0}` (`{1}`) VALUES ("{2}");'''.format("fuPan",index_str,value_str)
        print(sql)
        self.dbConnection.Execute(sql)
        
    def __str__(self) -> str:
        msg = f'''
        昨日10CM首板奖励率:{self.shouBan10CMRatio},
        昨日20CM首板奖励率:{self.shouBan20CMRatio},
        昨日10CM连板奖励率:{self.lianBan10CMRatio},
        昨日20CM连板奖励率:{self.lianBan20CMRatio},
        涨停家数:{self.countOfZhangTing},
        跌停家数:{self.countOfDieTing},
        高度板:{self.gaoDuBan},
        势能:{self.shiNeng},
        动能:{self.dongNeng:.2f},
        首板:               个数:{len(self.lianBan1)},
        2连板:              个数:{len(self.lianBan2)},
        3连板:              个数:{len(self.lianBan3)},          {";".join(self.lianBan3)},
        4连板以及以上:      个数:{len(self.lianBan4)},          {";".join(self.lianBan4)},
        '''
        return msg
    
    def MarketingData(self):
        df = GetTodayMarketingData(self.dbConnection,self.today)
        df["涨跌幅"] = df["涨跌幅"].astype(float)
        df['是否涨停'] = df.apply(lambda row: self._isZhangTing(row['股票代码'],row['涨跌幅']), axis=1)
        df['是否跌停'] = df.apply(lambda row: self._isDieTing(row['股票代码'],row['涨跌幅']), axis=1)
        df['是否ST'] = df.apply(lambda row: self._isST(row['股票简称']), axis=1)
        df = df[df['是否ST'] == False]
        df = df[df["上市天数"] > "10"]
        self.countOfZhangTing = df[df['是否涨停'] == True].shape[0]
        self.countOfDieTing = df[df['是否跌停'] == True].shape[0]
        self.hongPan = df[df["涨跌幅"]>0].shape[0]
        self.lvPan = df[df["涨跌幅"]<0].shape[0]
        print(df)
        print(list(df[df['是否ST'] == True]['股票简称']))
        print(list(df[df['是否涨停'] == True]['股票简称']))
        print(list(df[df['是否跌停'] == True]['股票简称']))
    
    def _isST(self,name):
        if name.find("ST")!=-1 or  name.find("st")!=-1:
            return True
        return False
    
    def _isZhangTing(self,stockID,zhangDieFu):
        zhangDieFu = float(zhangDieFu)
        if re.match('^00.*',stockID) is not None:
            if zhangDieFu >= 9.8:
                return True
            return False
        elif re.match('^30.*',stockID) is not None:
            if zhangDieFu >= 19.8:
                return True
            return False
        elif re.match('^60.*',stockID) is not None:
            if zhangDieFu >= 9.8:
                return True
            return False
        elif re.match('^68.*',stockID) is not None:
            if zhangDieFu >= 19.8:
                return True
            return False
        elif re.match('.*\BJ$',stockID) is not None:
            if zhangDieFu >= 29.8:
                return True
            return False
        else:
            return "unKnown"
            
    
    def _isDieTing(self,stockID,zhangDieFu):
        zhangDieFu = float(zhangDieFu)
        if re.match('^00.*',stockID) is not None:
            if zhangDieFu <= -9.8:
                return True
            return False
        elif re.match('^30.*',stockID) is not None:
            if zhangDieFu <= -19.8:
                return True
            return False
        elif re.match('^60.*',stockID) is not None:
            if zhangDieFu <= -9.8:
                return True
            return False
        elif re.match('^68.*',stockID) is not None:
            if zhangDieFu <= -19.8:
                return True
            return False
        elif re.match('.*\BJ$',stockID) is not None:
            if zhangDieFu <= -29.8:
                return True
            return False
        else:
            return "unKnown"


def FuPanFun():
    logger = StartToInitLogger("复盘")
    logger.info(f'==============begin:{datetime.datetime.now()}==============================')
    dbConnection = ConnectToDB()
    FuPan = CFuPan(logger,dbConnection,-1)
    FuPan.MarketingData()
    FuPan.FuPan()
    FuPan.FormatFuPanSqlAndToDB()
    logger.info(str(FuPan))
    NewGaiNian(dbConnection)
    logger.info(f'==============end:{datetime.datetime.now()}==============================')

def AutoDownload():
    schedule.every().day.at("17:35").do(FuPanFun)
    while(True):
        schedule.run_pending()
        time.sleep(1)

def Test():
    logger = StartToInitLogger("复盘_test")
    dbConnection = ConnectToDB()
    FuPan = CFuPan(logger,dbConnection,-1)
    FuPan.MarketingData()
    FuPan.FuPan()
    FuPan.FormatFuPanSqlAndToDB()
    print(FuPan)
    NewGaiNian(dbConnection)

def GetRecording():
    headers = {
        'sec-ch-ua': 'Not A;Brand;v=99, Chromium;v=98, Google Chrome;v=98 ',
        'accept': 'application/json, text/plain, */*',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36',
        'sec-ch-ua-platform': 'macOS',
        'origin': 'https://live.jtzyuan.com',
        'sec-fetch-site': 'same-site',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://live.jtzyuan.com/',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    maxNone = 0
    start = 10101343
    result = []
    res = {}
    for i in range(5000):
        api = f'https://vod.jtzyuan.com/api/queryVideoById?id={start+i}'
        response = requests.get(api,headers = headers)
        if response.status_code == 200:
            j = response.json()
            if maxNone >=10:
                break
            
            if j['result'] is None:
                maxNone = maxNone + 1
                continue
            maxNone = 1
            index = f"https://live.jtzyuan.com/special/vod/?id={start+i}#/"
            url = j['result']["url"]
            name = j['result']["name"]
            data = [start+i,index,name,api,url]
            result.append(data)
            if "盯盘抓板" in name or "复盘寻龙" in name or "大师直播课" in name:
                res[name] = index
                print(url)  
            print(f"{i:<4d} :   {name}  {api}")
        else:
            print(response.text)
            
    df = pd.DataFrame(result,columns=["id","index","name","API","URL"])
    df.to_excel('/tmp/recording.xlsx',index=False)
    
    for key in res:
        print(key, res[key])


def NewGaiNian(dbConnection=None):
    sql = "SELECT `所属概念`,`更新日期` FROM stock.stockbasicinfo;"
    datas,columns = dbConnection.Query(sql)
    #print(datas,columns)
    allGaiNian = []
    date = ""
    for data in datas:
        gaiNians = data[0]
        date = data[1]
        gaiNians = gaiNians.replace("--","")  
        gaiNian = gaiNians.split(";")
        allGaiNian.extend(gaiNian)
    
    allGaiNian = list(set(allGaiNian))

    tmp = []
    for gainian in allGaiNian:
        if len(gaiNian) == 0 or gaiNian == "":
            continue
        tmp.append(f'''("{gainian}","{date}")''')

    value_str = ''','''.join(tmp)
    sql = f'''INSERT IGNORE INTO `stock`.`gainian` (`概念名称`,`更新日期`) VALUES {value_str};'''
    print(sql)
    dbConnection.Execute(sql)

if __name__ == "__main__":
    #FuPanFun()
    GetRecording()