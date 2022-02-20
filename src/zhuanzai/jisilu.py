
import requests
import pandas as pd
import datetime
from mysql.connect2DB import ConnectToDB,DataFrameToSqls_REPLACE
from DBOperating import GetTradingDateLastN,GetKeZhuanzai,GetKeZhuanzai_remain

NAME_MAP = {
    "bond_nm":"转债名称",
    "bond_id":"转债代码",
    "price":"现价",
    "stock_id":"正股代码",
    "stock_nm":"正股名称",
    "pb":"PB",
    "int_debt_rate":"有息负债率",
    "pledge_rt":"股票质押率",
    "market_value":"流通市值（亿元)",
    "premium_rt":"溢价率",
    "sw_nm_r":"行业",
    "rating_cd":"评级",
    "put_convert_price":"回售触发价",
    "year_left":"剩余年限",
    "curr_iss_amt":"剩余规模",
    "ytm_rt":"到期税前收益率",
    "bond_nm_tip":"提示",
}


class CJiSiLu(object):
    def __init__(self,logger,dbConnection) -> None:
        super().__init__()
        self.logger = logger
        self.dbConnection = dbConnection
        self.cookie = "kbzw__Session=bao8cb79jtucr7dg42iu1h6h14; kbzw_r_uname=%E6%9E%97%E6%9E%97%E5%AD%90; Hm_lvt_164fe01b1433a19b507595a43bf58262=1641813690; kbz_newcookie=1; kbzw__user_login=7Obd08_P1ebax9aXXQANRxUOWCXxkZyh6dbc7OPm1Nq_1KKor5Ogk9irp5qp0Nmaq8WuqaTbmqiW2rCmnLCdrZSomJiyoO3K1L_RpKuZqZ2umZecpMy01r6bruPz3tXlzaaXpJGXn8DZxNnP6Ojo0bSMwNDqxuOXwNnEkLDHmc2JqpzWk6vArqCTudHgzdnQ2svE1euRq5SupaaugZisvM3CtaWM48vhxpe-2NvM34qUvN3b6Nncka-YpaehrJepk6KwponMzd3D6MqmrKavj6OX; Hm_lpvt_164fe01b1433a19b507595a43bf58262=1644239925"
    
    def request1_login(self):
        self.logger.info(f'==============begin:{datetime.datetime.now()}==============================')
        head = {
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0",
        "sec-ch-ua": "Not;A Brand;v=99, Google Chrome;v=97, Chromium;v=97",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "macOS",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-User": "?1",
        "Sec-Fetch-Dest": "document",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cookie":f"{self.cookie}",
        }
        url='https://www.jisilu.cn/'
        response = requests.request("GET",url,headers=head)
        self.logger.info(response.status_code)
        self.logger.info(response.text)
        self.logger.info(f'==============end:{datetime.datetime.now()}==============================')
        
    def jisilu(self):
        self.logger.info(f'==============begin:{datetime.datetime.now()}==============================')
        sse_head = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'Columns':'1,70,2,3,5,6,7,8,9,10,11,12,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,44,46,47,50,52,53,54,55,56,57,58,59,60,62,63,64,67,71,69',
        "Connection": "keep-alive",
        "Host": "www.jisilu.cn",
        "Init": "1",
        'Referer': 'https://www.jisilu.cn/web/data/cb/list',
        "sec-ch-ua": "Not;A Brand;v=99, Google Chrome;v=97, Chromium;v=97",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "macOS",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        # "If-Modified-Since": "Sun, 16 Jan 2022 01:10:13 GMT",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
        "Cookie":f"{self.cookie}",
        }
        url='https://www.jisilu.cn/webapi/cb/list_new/'
        response = requests.request("GET",url,headers=sse_head,verify = False)
        #print(response, response.text)
        if len(response.text)!=0:
            j = response.json()
            print(j['code'])
            data = j['data']
            df = pd.DataFrame(data)
            df = df[df['price_tips']!="待上市"]
            #print(df.columns)
            
            newDf = pd.DataFrame()
            columns = NAME_MAP.keys()
            for column in columns:
                newDf[NAME_MAP[column]] = df[column]
            #print(newDf)
            
            newDf.loc[newDf['流通市值（亿元)']<=50, '流通市值小于50亿'] = '小于50亿'
            newDf.loc[newDf['剩余规模']<=3, '剩余规模<=3'] = '剩余规模<=3'
            newDf['PB-溢价率'] = (newDf['PB'] - newDf['溢价率']/100.0)
            #newDf['流通市值小于50亿'] = (newDf['流通市值（亿元)']<=50)
            #newDf['剩余规模<=3'] = (newDf['剩余规模']<=3)
            newDf.to_excel('/tmp/aa.xlsx',index=False)
            newDf = newDf[newDf['现价']<=135]
            newDf = newDf[newDf['PB']>=1.2]
            newDf = newDf[newDf['有息负债率']>0]
            newDf = newDf[newDf['有息负债率']<70]
            newDf = newDf[newDf['股票质押率']>0]
            newDf = newDf[newDf['流通市值（亿元)']<=250]
            #newDf = newDf[newDf['PB-溢价率']>=1.0]
            newDf = newDf[newDf['评级'].isin(["AAA","AA+","AA","AA-","A+"])]
            newDf = newDf[newDf['回售触发价']>0]
            newDf = newDf[newDf['剩余年限']>1]
            newDf.sort_values('PB',axis=0,ascending=False,inplace=True)
            self.logger.info(f'{newDf}')
            self.logger.info(f'{newDf.shape}')
            self.logger.info(f'==============end:{datetime.datetime.now()}==============================')
            return newDf
        
    def GetFromJisiluAndWriteToDB(self):
        today = datetime.date.today()
        dates = GetTradingDateLastN(self.dbConnection,10)
        if today not in dates:
            today = dates[-1]
        self.logger.info(today)
        self.request1_login()
        df = self.jisilu()
        if df is not None:
            df['日期'] = today
            sqls = DataFrameToSqls_REPLACE(df,"kezhuanzhai")
            for sql in sqls:
                self.logger.info(sql)
                self.dbConnection.Execute(sql)
        
        lastDay = dates[-2]
        sql1 = f"SELECT A.*, B.`所属概念` FROM `stock`.`kezhuanzhai` as A,`stock`.`stockBasicInfo` AS B where A.`正股名称`=B.`股票简称` and `日期`='{today}' and `转债代码` not in (SELECT `转债代码` FROM kezhuanzhai where `日期`='{lastDay}') order by `PB` DESC;"
        result1,columns1 = self.dbConnection.Query(sql1)
        newDf1=pd.DataFrame(result1,columns=columns1)
        
        sql2 = f"SELECT A.*, B.`所属概念` FROM `stock`.`kezhuanzhai` as A,`stock`.`stockBasicInfo` AS B where A.`正股名称`=B.`股票简称` and `日期`='{lastDay}' and `转债代码` not in (SELECT `转债代码` FROM kezhuanzhai where `日期`='{today}') order by `PB` DESC;"
        result2,columns2 = self.dbConnection.Query(sql2)
        newDf2=pd.DataFrame(result2,columns=columns2)
        name = '/Volumes/Data/Downloads/可转债每日选股/%s_变化量.xlsx'%(datetime.date.today())
        excelWriter = pd.ExcelWriter(name)
        newDf1.to_excel(excelWriter,"今日增加",index=False)
        newDf2.to_excel(excelWriter,"今日减少",index=False)
        excelWriter.save()

    def Categrate(self,categrateMap):
        result = GetTradingDateLastN(self.dbConnection,15)
        date = result[-1]
        
        remain = []
        for key in categrateMap:
            self.logger.info(f"\n=========={key}=============")
            df = GetKeZhuanzai(self.dbConnection,date,categrateMap[key])
            remain.extend(categrateMap[key])
            self.logger.info(str(df))
        
        self.logger.info(f"\n==========剩余=============")
        df = GetKeZhuanzai_remain(self.dbConnection,date,remain)
        self.logger.info(str(df))