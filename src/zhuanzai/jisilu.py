
import requests
import pandas as pd
import datetime
from mysql.connect2DB import ConnectToDB,DataFrameToSqls_REPLACE
from DBOperating import GetTradingDateLastN,GetKeZhuanzai,GetKeZhuanzai_remain
import os
import sys
from workspace import workSpaceRoot,WorkSpaceFont
import re
import time

NAME_MAP = {
    "bond_nm":"转债名称",
    "bond_id":"转债代码",
    "price":"现价",
    "volume":"成交额(万元)",
    "stock_id":"正股代码",
    "stock_nm":"正股名称",
    "svolume":"正股成交额(万元)",
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


NAME_MAP_Newstock = {
"stock_cd":"股票代码",
"stock_nm":"股票名称",
"apply_dt":"申购日",
"apply_dt2":"申购日期",
"need_market_value":"需配市值",
"apply_cd":"申购代码",
"issue_price":"发行价",
"individual_limit":"申购限额",
"money_out_dt":"缴款日",
"lucky_draw_rt":"中签率",
"list_dt":"上市日期",
"after_issue_show":"发行时总市值",
"issue_show":"公开发行市值",
"pub_pe":"发行市盈率",
"avg_pe":"行业市盈率",
"theory_price":"开板收盘价",
"theory_profit":"单签收益",
"underwriter":"承销商"
}

pd.set_option('display.unicode.ambiguous_as_wide',True)
pd.set_option('display.unicode.east_asian_width',True)
pd.set_option('display.width',360)

class CJiSiLu(object):
    def __init__(self,logger,dbConnection) -> None:
        super().__init__()
        self.logger = logger
        self.dbConnection = dbConnection
        self.today = None
        self.kbzw_user_login = "7Obd08_P1ebax9aXXQANRxUOWCXxkZyh6dbc7OPm1Nq_1KLZ25GhkafdrZvfz6uZ3JWsrNTcx6WWqq6lm92lrJrYw5iyoO3K1L_RpKuZqZ2umZecpLjH1r6bkqqyrZ2ppLCarIKypMi5v82Mwejv0uXY2JGrj6eXm8XC08ri7eTc4aeXq-TV3OOTxcLTgcPMlcGZnafBp5bWrpyYouDR4N7Mztu34NallqquoauXkIm_wcm2xZiXzt_M3Je63cTb0J2ZuNHr2-THpZKor6Goj6CPpJnIyt_N6cullqquoauX"
        self.cookie = f"kbz_newcookie=1; kbzw__Session=9aqo0c4fm6dh30cv9lgck4n1l1; Hm_lvt_164fe01b1433a19b507595a43bf58262=1698406312; kbzw__user_login={self.kbzw_user_login}; Hm_lpvt_164fe01b1433a19b507595a43bf58262=1698412256"
    

    def getCookies(self):
        sql = f'''SELECT cookie FROM cookies where name = "jisilu";'''
        results, _ = self.dbConnection.Query(sql)
        self.cookie = results[0][0]

    def _formatResult(self,row):
        result = ""
        if row['现价'] > 125:
            result = result + '{0:{3}<10}\t{1:{3}<8}\t{2:<15}\n'.format('价    格:',row['现价'],"[<=125];",chr(12288),end = '')

        if row['PB'] < 1.2:
            result = result + '{0:{3}<10}\t{1:{3}<8}\t{2:<15}\n'.format('平均市净率:',row['PB'],"[>=1.2];",chr(12288),end = '')

        if row['有息负债率'] > 70 or row['有息负债率']<0:
            result = result + '{0:{3}<10}\t{1:{3}<8}\t{2:<15}\n'.format('有息负债率:',row['有息负债率'],"[0<x<70];",chr(12288),end = '')

        # if row['股票质押率'] < 0:
        #     result = result + '{0:{3}<10}\t{1:{3}<8}\t{2:<15}\n'.format('股票质押率:',row['股票质押率'],"[>=0];",chr(12288),end = '')
        
        if row['流通市值（亿元)'] > 250:
            result = result + '{0:{3}<10}\t{1:{3}<8}\t{2:<15}\n'.format('流通市值:',row['流通市值（亿元)'],"[<=250];",chr(12288),end = '')

        if row['PB-溢价率'] < 1:
            result = result + '{0:{3}<10}\t{1:.2f}\t{2:<15}\n'.format('市净-溢价:',float(row['PB-溢价率']),"[>=1.0];",chr(12288),end = '')

        if row['评级'] not in ["AAA","AA+","AA","AA-","A+"]:
            result = result + '{0:{3}<10}\t{1:{3}<8}\t{2:<15}\n'.format('评   级:',row['评级'],"[AAA,AA+,AA,AA-,A+];",chr(12288),end = '')
        
        if row['回售触发价'] <= 0:
            result = result + '{0:{3}<10}\t{1:{3}<8}\t{2:<15}\n'.format('回售触发价:',row['回售触发价'],"[>0];",chr(12288),end = '')

        if row['剩余年限'] <= 1:
            result = result + '{0:{3}<10}\t{1:{3}<8}\t{2:<15}\n'.format('剩余年限:',row['剩余年限'],"[>1];",chr(12288),end = '')

        if row['剩余规模'] < 3.5:
            result = result + '{0:{3}<10}\t{1:{3}<8}\t{2:<15}\n'.format('剩余规模:',row['剩余规模'],"[>=3.5];",chr(12288),end = '')

        alarmPattern = '[\s\S]*(公告要强赎|临近到期|最后交易日|最后转股日)+?[\s\S]*'
        if re.match(alarmPattern,row['提示']) != None:
            result = result + '{0:{3}<10}\t{1:{3}<8}\t{2:<15}\n'.format('公告:',row['提示'].strip().split('\n')[0].split('：')[0],"[无强赎公告];",chr(12288),end = '')

        #print(result)
        return result


    def jisilu(self):
        self.logger.info(f'==============begin:{datetime.datetime.utcnow()}==============================')
        sse_head = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'Columns':'1,70,2,3,5,6,7,8,9,10,11,12,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,44,46,47,50,52,53,54,55,56,57,58,59,60,62,63,64,67,71,69',
        "Connection": "keep-alive",
        "Host": "www.jisilu.cn",
        "Init": "1",
        'Referer': 'https://www.jisilu.cn/web/data/cb/list',
        "sec-ch-ua": "\"Chromium\";v=\"118\", \"Google Chrome\";v=\"118\", \"Not=A?Brand\";v=\"99\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "macOS",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        # "If-Modified-Since": "Sun, 16 Jan 2022 01:10:13 GMT",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
        "Cookie":f"{self.cookie}",
        "if-modified-since":"Fri, 27 Oct 2023 12:57:15 GMT",
        }
        url='https://www.jisilu.cn/webapi/cb/list/'
        response = requests.request("GET",url,headers=sse_head,verify = False)
        #print(response, response.text)
        if len(response.text)!=0:
            j = response.json()
            print(j['code'])
            print(j['prompt'])
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
            newDf['回售触发价'] =  newDf['回售触发价'].round(2)
            newDf['提示'] =  newDf['提示'].str.replace('\r\n',"")
            #newDf['流通市值小于50亿'] = (newDf['流通市值（亿元)']<=50)
            #newDf['剩余规模<=3'] = (newDf['剩余规模']<=3)

            df_all = newDf.copy()
            #     df['新成交额'] = df.apply(lambda row: formatVolumn(row['成交额'],1.0), axis=1)
            df_all["筛选结果"] = df_all.apply(lambda row: self._formatResult(row), axis=1)

            df_all['日期'] = self.today
            folder = f"{workSpaceRoot}/复盘/可转债/{self.today}/"
            if os.path.exists(folder) == False:
                os.makedirs(folder)
            fName = f"{workSpaceRoot}/复盘/可转债/{self.today}/每日原始数据_{self.today}.xlsx"
            sqls = DataFrameToSqls_REPLACE(df_all,"kezhuanzhai_all")
            for sql in sqls:
                if self.dbConnection.Execute(sql) == False:
                    sys.exit(1)

            newDf.to_excel(fName,index=False)

            newDf = newDf[newDf['现价']<=125]
            newDf = newDf[newDf['PB']>=1.2]
            newDf = newDf[newDf['有息负债率']>0]
            newDf = newDf[newDf['有息负债率']<70]
            #newDf = newDf[newDf['股票质押率']>=0]
            newDf = newDf[newDf['流通市值（亿元)']<=250]
            #newDf = newDf[newDf['PB-溢价率']>=1.0]
            newDf = newDf[newDf['评级'].isin(["AAA","AA+","AA","AA-","A+"])]
            newDf = newDf[newDf['回售触发价']>0]
            newDf = newDf[newDf['剩余年限']>1]
            newDf = newDf[newDf['剩余规模']>0.5]
            newDf = newDf[newDf['提示'].str.contains('[\s\S]*(公告要强赎|临近到期|最后交易日|最后转股日)+?[\s\S]*') == False]
            newDf.sort_values('PB',axis=0,ascending=False,inplace=True)
            self.logger.info(f'{newDf}')
            self.logger.info(f'{newDf.shape}')
            self.logger.info(f'==============end:{datetime.datetime.utcnow()}==============================')
            return newDf

    def ConvertDataFrameToJPG(self,df,fullPath):
        from pandas.plotting import table
        import matplotlib.pyplot as plt
        if df.empty == True:
            return
        plt.rcParams["font.sans-serif"] = [WorkSpaceFont]#显示中文字体
        high = int(0.174 * df.shape[0]+0.5) +1
        fig = plt.figure(figsize=(3, high), dpi=400)#dpi表示清晰度
        ax = fig.add_subplot(111, frame_on=False) 
        ax.xaxis.set_visible(False)  # hide the x axis
        ax.yaxis.set_visible(False)  # hide the y axis
        table(ax, df, loc='center')  # 将df换成需要保存的dataframe即可
        plt.savefig(fullPath)
    
    def GetFromJisiluAndWriteToDB(self):
        self.today = datetime.date.today()
        dates = GetTradingDateLastN(self.dbConnection,10)
        if self.today not in dates:
            self.today = dates[-1]

        self.logger.info(self.today)
        self.getCookies()
        #self.request1_login()
        #self.request2_login()
        df = self.jisilu()
        df.reset_index(drop=True,inplace=True)
        
        jpgDataFrame = pd.DataFrame(df,columns=["转债代码","转债名称"])

        folderRoot= f'''{workSpaceRoot}/复盘/可转债/{self.today}/'''
        if os.path.exists(folderRoot) == False:
            os.makedirs(folderRoot)

        self.ConvertDataFrameToJPG(jpgDataFrame,f"{folderRoot}{self.today}_all.jpg")
        
        size = df.shape[0]
        step = 40
        if size > step:
            for index in range(0,size,step):
                tmp = df.iloc[index:,]
                if index + step <= size:
                    tmp = df.iloc[index:index+step,]
                fullPath = f"{folderRoot}{self.today}_{int(index/step+1)}.jpg"
                print(fullPath)
                jpgDataFrame = pd.DataFrame(tmp,columns=["转债代码","转债名称"])
                self.ConvertDataFrameToJPG(jpgDataFrame,fullPath)

        if df is not None:
            df['日期'] = self.today
            sqls = DataFrameToSqls_REPLACE(df,"kezhuanzhai")
            for sql in sqls:
                #self.logger.info(sql)
                if self.dbConnection.Execute(sql) == False:
                    sys.exit(1)
        
        lastDay = dates[-2]
        sql1 = f"SELECT A.*, B.`所属概念` FROM `stock`.`kezhuanzhai` as A,`stock`.`stockBasicInfo` AS B where A.`正股名称`=B.`股票简称` and `日期`='{self.today}' and `转债代码` not in (SELECT `转债代码` FROM kezhuanzhai where `日期`='{lastDay}') order by `PB` DESC;"
        result1,columns1 = self.dbConnection.Query(sql1)
        newDf1=pd.DataFrame(result1,columns=columns1)
        
        sql2 = f"SELECT A.*, B.`所属概念` FROM `stock`.`kezhuanzhai` as A,`stock`.`stockBasicInfo` AS B where A.`正股名称`=B.`股票简称` and `日期`='{lastDay}' and `转债代码` not in (SELECT `转债代码` FROM kezhuanzhai where `日期`='{self.today}') order by `PB` DESC;"
        result2,columns2 = self.dbConnection.Query(sql2)
        newDf2=pd.DataFrame(result2,columns=columns2)
        name = f'{folderRoot}{self.today}_变化量.xlsx'
        with pd.ExcelWriter(name,engine='openpyxl',mode='w+') as excelWriter:
            newDf1.to_excel(excelWriter,"今日增加",index=False)
            newDf2.to_excel(excelWriter,"今日减少",index=False)

    def Categrate(self,categrateMap):
        result = GetTradingDateLastN(self.dbConnection,15)
        date = result[-1]
        
        remain = []

        folderRoot= f'''{workSpaceRoot}/复盘/可转债/{self.today}/'''
        if os.path.exists(folderRoot) == False:
            os.makedirs(folderRoot)

        
        for key in categrateMap:
            self.logger.info(f"\n=========={key}=============")
            df = GetKeZhuanzai(self.dbConnection,date,categrateMap[key])
            remain.extend(categrateMap[key])
            jpgDataFrame = pd.DataFrame(df,columns=["转债代码","转债名称"])
            self.ConvertDataFrameToJPG(jpgDataFrame,f"{folderRoot}{self.today}_{key}.jpg")
            #self.logger.info(str(df))
        
        self.logger.info(f"\n==========剩余=============")
        df = GetKeZhuanzai_remain(self.dbConnection,date,remain)
        jpgDataFrame = pd.DataFrame(df,columns=["转债代码","转债名称"])
        self.ConvertDataFrameToJPG(jpgDataFrame,f"{folderRoot}{self.today}_剩余.jpg")
        #self.logger.info(str(df))


########################################################################################################################
#新股日历
    
    def GetNewStockCalendar(self):
        self.logger.info(f'==============新股日历 begin:{datetime.datetime.utcnow()}==============================')
        sse_head = {
        'authority': 'www.jisilu.cn',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type':'application/x-www-form-urlencoded; charset=UTF-8',

        "origin": "https://www.jisilu.cn",
        'referer': 'https://www.jisilu.cn/data/new_stock/',


        "Host": "www.jisilu.cn",
        "Init": "1",
        
        "sec-ch-ua": "\"Chromium\";v=\"119\", \"Google Chrome\";v=\"119\", \"Not=A?Brand\";v=\"24\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "macOS",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",

        # "If-Modified-Since": "Sun, 16 Jan 2022 01:10:13 GMT",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Cookie":f"{self.cookie}",
        "x-requested-with":"XMLHttpRequest",
        }
        t = int(time.time()*1000)
        url=f'''https://www.jisilu.cn/data/new_stock/apply/?___jsl=LST___t={t}'''

        payload = "market%5B%5D=shmb&market%5B%5D=shkc&market%5B%5D=szmb&market%5B%5D=szcy&market%5B%5D=bj&rp=22&page=1&pageSize=1000"
        response = requests.request("GET",url,headers=sse_head, data= payload)
        #print(response, response.text)
        if len(response.text)!=0:
            j = response.json()
            rows = j['rows']
            datas = []
            for row in rows:
                cell = row['cell']
                datas.append(cell)

            df = pd.DataFrame(datas)
            newDf = pd.DataFrame()
            columns = NAME_MAP_Newstock.keys()
            for column in columns:
                newDf[NAME_MAP_Newstock[column]] = df[column]
            print(newDf)

            sqls = DataFrameToSqls_REPLACE(newDf,"newstocks")
            for sql in sqls:
                if self.dbConnection.Execute(sql) == False:
                    sys.exit(1)