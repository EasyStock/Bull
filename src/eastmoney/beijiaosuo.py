import requests
import json
import pandas as pd


class CBeiJiaoSuoDataFetcher(object):
    def __init__(self):
        pass


    def _fetchData(self,page =1):
        head = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
            "cache-control":"no-cache",
            "pragma": "no-cache",
            #"Cookie": "st_si=33930137927479; FundWebTradeUserInfo=JTdCJTIyQ3VzdG9tZXJObyUyMjolMjIlMjIsJTIyQ3VzdG9tZXJOYW1lJTIyOiUyMiUyMiwlMjJWaXBMZXZlbCUyMjolMjIlMjIsJTIyTFRva2VuJTIyOiUyMiUyMiwlMjJJc1Zpc2l0b3IlMjI6JTIyJTIyLCUyMlJpc2slMjI6JTIyJTIyJTdE; qgqp_b_id=07e364420df0ba22c7e879b4c301457d; EMFUND1=null; EMFUND2=null; EMFUND3=null; EMFUND4=null; EMFUND5=null; EMFUND6=null; EMFUND7=null; EMFUND8=null; EMFUND0=null; EMFUND9=05-15 16:21:41@#$%u957F%u57CE%u4E2D%u503A1-3%u5E74%u653F%u91D1%u503AA@%23%24008652; st_asi=delete; st_pvi=10101638801112; st_sp=2022-05-15%2015%3A58%3A45; st_inirUrl=http%3A%2F%2Ffund.eastmoney.com%2Fdata%2F; st_sn=37; st_psi=20220515172651988-113300300999-4804406827; JSESSIONID=55F29FD72541EE153DAF30E7F0F13091",
            "Host": "datacenter-web.eastmoney.com",
            "Referer": "https://data.eastmoney.com/xg/xg/?mkt=bjs",
            "Sec-Fetch-Dest": "script",
            "Sec-Fetch-Mode": "no-cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "sec-ch-ua": "Not A;Brand ;v=99, Chromium;v=130, Google Chrome;v=130",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "macOS",
        }
        callback = "jQuery1123020050835417527524_1730374301421"
        url = f'''https://datacenter-web.eastmoney.com/api/data/v1/get?callback={callback}&sortColumns=APPLY_DATE&sortTypes=-1&pageSize=50&pageNumber={page}&columns=ALL&reportName=RPT_NEEQ_ISSUEINFO_LIST&quoteColumns=f14%2Cf2~01~SECURITY_CODE%2Cf3~01~SECURITY_CODE%2CNEW_CHANGE_RATE~01~SECURITY_CODE&quoteType=0&source=NEEQSELECT&client=WEB'''

        response = requests.request("GET",url,headers=head)
        js = response.text[len(callback)+1:-2]
        js = json.loads(js)
        count = js["result"]["count"]
        pages = js["result"]["pages"]
        df = pd.DataFrame.from_dict(js["result"]["data"])
        newDF = pd.DataFrame()
        newDF["股票代码"] = df["SECUCODE"]
        newDF["股票名称"] = df["SECURITY_NAME_ABBR"]
        newDF["发行总数(股)"] = df["EXPECT_ISSUE_NUM"]
        newDF["发行价格"] = df["ISSUE_PRICE_ADJFACTOR"].astype(float).apply(lambda x:'''%.2f'''%x).astype(float)
        newDF["顶格申购金额"] = df["APPLY_AMT_UPPER"].astype(float)
        newDF["申购年份"] = df["APPLY_DATE"].str[:4]
        newDF["申购日期"] = df["APPLY_DATE"].str[:10]
        newDF["中签率"] = df["ONLINE_ISSUE_LWR"].astype(float).apply(lambda x:'''%.2f'''%x).astype(float)
        newDF["稳获百股所需资金(元)"] = df["APPLY_AMT_100"].astype(float).apply(lambda x:'''%.2f'''%x).astype(float)
        newDF["上市日期"] = df["SELECT_LISTING_DATE"].str[:10]
        newDF["上市均价"] = df["AVERAGE_PRICE"].astype(float).apply(lambda x:'''%.2f'''%x).astype(float)
        newDF["平均涨幅"] = df["LD_CLOSE_CHANGE"].astype(float).apply(lambda x:'''%.2f'''%x).astype(float)
        newDF["每百股平均获利(元)"] = df["PER_SHARES_INCOME"].astype(float).apply(lambda x:'''%.2f'''%x).astype(float)
        return (newDF,count,pages)


    def FetcheData(self,dbConnection):
        newDF, count,pages = self._fetchData()
        for page in range(2,pages+1):
            newPageDF, _, _ = self._fetchData(page)
            newDF = pd.concat([newDF,newPageDF])

        if newDF.shape[0] != count:
            raise("count error")
        
        newDF.dropna(inplace=True)
        newDF[["上市当日开盘价","上市当日收盘价"]]  = newDF.apply(lambda row: self._cat(dbConnection,row), axis=1,result_type="expand")
        newDF.to_excel("/tmp/bbbb.xlsx")

        newDF["开盘价百股收益"] = (newDF["上市当日开盘价"] - newDF["发行价格"])*100
        newDF["收盘价百股收益"] = (newDF["上市当日收盘价"] - newDF["发行价格"])*100

        # newDF["开盘价百股收益"] = newDF["开盘价百股收益"].astype(float).apply(lambda x:'''%.2f'''%x)
        # newDF["收盘价百股收益"] = newDF["收盘价百股收益"].astype(float).apply(lambda x:'''%.2f'''%x)

        sqls = self._DataFrameToSqls_INSERT_OR_REPLACE(newDF, "beijiaosuo")
        step = 100
        grouped_sqls = [" ".join(sqls[i:i+step]) for i in range(0,len(sqls),step)]
        for sql in grouped_sqls:
            dbConnection.Execute(sql)

    
    def _cat(self,dbConnection,row):
        stockID = row["股票代码"]
        date = row["上市日期"]
        sql = f'''SELECT `股票代码`,`日期`,`开盘价`,`收盘价` FROM stock.stockdailyinfo where `日期` = "{date}" and  `股票代码` = "{stockID}" 
        UNION ALL
        SELECT `股票代码`,`日期`,`开盘价`,`收盘价` FROM stock.stockdailyinfo_2023 where `日期` = "{date}" and  `股票代码` = "{stockID}"
        UNION ALL
        SELECT `股票代码`,`日期`,`开盘价`,`收盘价` FROM stock.stockdailyinfo_2022 where `日期` = "{date}" and  `股票代码` = "{stockID}"
        UNION ALL
        SELECT `股票代码`,`日期`,`开盘价`,`收盘价` FROM stock.stockdailyinfo_2021 where `日期` = "{date}" and  `股票代码` = "{stockID}"
        '''
        data,_= dbConnection.Query(sql)
        if len(data) == 0:
            return (0,0)
        else:
            return (float(data[0][2]),float(data[0][3]))

    def _DataFrameToSqls_INSERT_OR_REPLACE(self,datas,tableName):
        sqls = []
        for _, row in datas.iterrows():
            index_str = '''`,`'''.join(row.index)
            value_str = '''","'''.join(str(x) for x in row.values)
            sql = '''REPLACE INTO `{0}` (`{1}`) VALUES ("{2}");'''.format(tableName,index_str,value_str)
            sqls.append(sql)
        return sqls
    
    
    def _count(self,row,money):
        maxMoney = row["顶格申购金额"]
        p = row["稳获百股所需资金(元)"]
        m = min(maxMoney,money)
        return int(m/p)

    def shouYi(self,dbConnection,year = None,money = 2000000):
        if year is None:
            sql = f'''SELECT * FROM stock.beijiaosuo'''
        else:
            sql = f'''SELECT * FROM stock.beijiaosuo where `申购年份` = "{year}"'''

        data,columns= dbConnection.Query(sql)
        df = pd.DataFrame(data=data, columns=columns)
        df = df[df["上市当日开盘价"] > 0]
        df = df[df["上市当日收盘价"] > 0]

        df["中签手数"] = df.apply(lambda row: self._count(row,money), axis=1)
        df["平均百股收益"] = df["中签手数"]*df["每百股平均获利(元)"]
        df["开盘价百股收益"] = df["中签手数"]*df["开盘价百股收益"]
        df["收盘价百股收益"] = df["中签手数"]*df["收盘价百股收益"]

        total1 = df["平均百股收益"].sum()
        total2 = df["开盘价百股收益"].sum()
        total3 = df["收盘价百股收益"].sum()
        return (total1,total2,total3)
    
