

# GET /commonQuery.do?jsonCallBack=jsonpCallback80339227&isPagination=true&pageHelp.pageSize=25&pageHelp.pageNo=1&pageHelp.beginPage=1&pageHelp.cacheSize=1&pageHelp.endPage=1&sqlId=COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L&STAT_DATE=&_=1737807549898 HTTP/1.1
# Accept: */*
# Accept-Encoding: gzip, deflate, br, zstd
# Accept-Language: zh-CN,zh;q=0.9
# Connection: keep-alive
# Cookie: ba17301551dcbaf9_gdp_session_id=33feb1e3-90c5-49b0-821c-527f547017b1; gdp_user_id=gioenc-3eb6d3g4%2Ce663%2C5g74%2C8117%2C77c17a755b0d; ba17301551dcbaf9_gdp_session_id_sent=33feb1e3-90c5-49b0-821c-527f547017b1; ba17301551dcbaf9_gdp_sequence_ids={%22globalKey%22:31%2C%22VISIT%22:2%2C%22PAGE%22:7%2C%22VIEW_CLICK%22:24}
# Host: query.sse.com.cn
# Referer: https://www.sse.com.cn/
# Sec-Fetch-Dest: script
# Sec-Fetch-Mode: no-cors
# Sec-Fetch-Site: same-site
# User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36
# sec-ch-ua: "Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"
# sec-ch-ua-mobile: ?0
# sec-ch-ua-platform: "macOS"


# GET /commonQuery.do?jsonCallBack=jsonpCallback84651614&isPagination=true&pageHelp.pageSize=25&pageHelp.pageNo=2&pageHelp.beginPage=2&pageHelp.cacheSize=1&pageHelp.endPage=2&sqlId=COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L&STAT_DATE=&_=1737807549900 HTTP/1.1
# Accept: */*
# Accept-Encoding: gzip, deflate, br, zstd
# Accept-Language: zh-CN,zh;q=0.9
# Connection: keep-alive
# Cookie: ba17301551dcbaf9_gdp_session_id=33feb1e3-90c5-49b0-821c-527f547017b1; gdp_user_id=gioenc-3eb6d3g4%2Ce663%2C5g74%2C8117%2C77c17a755b0d; ba17301551dcbaf9_gdp_session_id_sent=33feb1e3-90c5-49b0-821c-527f547017b1; ba17301551dcbaf9_gdp_sequence_ids={%22globalKey%22:33%2C%22VISIT%22:2%2C%22PAGE%22:7%2C%22VIEW_CLICK%22:26}
# Host: query.sse.com.cn
# Referer: https://www.sse.com.cn/
# Sec-Fetch-Dest: script
# Sec-Fetch-Mode: no-cors
# Sec-Fetch-Site: same-site
# User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36
# sec-ch-ua: "Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"
# sec-ch-ua-mobile: ?0
# sec-ch-ua-platform: "macOS"


import requests
import json
import pandas as pd
import logging
import time
import random

logger = logging.getLogger()

class CFetchETFDailyData(object):
    def __init__(self,dbConnection = None,date = None) :
        self.dbConnection = dbConnection
        self.allDataCount = None
        self.moreUrl = None
        self.date = date
        self.totalPages = None
        self.totalSize = None
        self.rowDatas = []


    def pageURL(self,page,date):
        random_number = random.randint(10000000, 99999999)
        jsonCallBack = f'''jsonpCallback{random_number}'''
        time_now = int(time.time())*1000
        url = f'''https://query.sse.com.cn/commonQuery.do?jsonCallBack={jsonCallBack}&isPagination=true&pageHelp.pageSize=25&pageHelp.pageNo={page}&pageHelp.beginPage={page}&pageHelp.cacheSize=1&pageHelp.endPage={page}&sqlId=COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L&STAT_DATE={date}&_={time_now}'''
        return (jsonCallBack,url)
    
    def RequestData(self,page):
        if self.date is None:
            self.date = time.strftime("%Y-%m-%d", time.localtime())

        jsonCallBack,url = self.pageURL(page,self.date)
        headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Host": "query.sse.com.cn",
        "Referer": "https://www.sse.com.cn/",
        "Sec-Fetch-Dest": "script",
        "Sec-Fetch-Mode": "no-cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "sec-ch-ua": "Chromium;v=128,Not;A=Brand;v=24, Google Chrome;v=128",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "macOS",
        }

        try:
            response = requests.request("GET",url,headers=headers)
            jsonData = response.text[len(jsonCallBack)+1:-1]
            js =json.loads(jsonData)
            if self.totalPages is None:
                self.totalPages = js["pageHelp"]["pageCount"]
                self.totalSize = js["pageHelp"]["total"]
                print(f"正在获取 {self.date} ETF份额数据:共{self.totalSize}条数据，共{self.totalPages}页, 当前是第{page}页")
            else:
                print(f"正在获取 {self.date} ETF份额数据:共{self.totalSize}条数据，共{self.totalPages}页, 当前是第{page}页")
            data = js["pageHelp"]["data"]
            self.rowDatas.extend(data)
        except Exception as e:
            print(e)

    def formatVolumn2(self,volumn):
        ret = f'''{float(volumn):.2f}'''
        return ret
    
    def FetchAllDatas(self):
        if self.dbConnection is None:
            return 
        
        self.RequestData(1)
        time.sleep(3)
        for page in range(2, self.totalPages+1):
            self.RequestData(page)
            time.sleep(3)

        df = pd.DataFrame(self.rowDatas)
        resDf = pd.DataFrame()
        resDf["日期"] = df["STAT_DATE"]
        resDf["基金代码"] = df["SEC_CODE"]
        resDf["类型"] = df["ETF_TYPE"]
        resDf["基金扩位简称"] = df["SEC_NAME"]
        resDf["总份额(万份)"] = df["TOT_VOL"]
        
        resDf['总份额(万份)'] = resDf.apply(lambda row: self.formatVolumn2(row['总份额(万份)']), axis=1)
        if resDf.shape[0] != self.totalSize:
            raise Exception("data size is not correct!")
        
        sqls = self._DataFrameToSqls_INSERT_OR_REPLACE(resDf,"etf_dailyinfo")
        step = 200
        groupedSql = [" ".join(sqls[i:i+step]) for i in range(0,len(sqls),step)]
        for sql in groupedSql:
            self.dbConnection.Execute(sql)

    def _DataFrameToSqls_INSERT_OR_REPLACE(self,datas,tableName):
        sqls = []
        for _, row in datas.iterrows():
            index_str = '''`,`'''.join(row.index)
            value_str = '''","'''.join(str(x) for x in row.values)
            sql = '''REPLACE INTO `{0}` (`{1}`) VALUES ("{2}");'''.format(tableName,index_str,value_str)
            sqls.append(sql)
        return sqls
    

if __name__ == "__main__":
    ft = CFetchETFDailyData(dbConnection = None,date = "2025-01-22")
    ft.FetchAllDatas()