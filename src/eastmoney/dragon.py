from re import S
import requests
import pandas as pd
import json




class CDragonFetcher(object):
    def __init__(self) -> None:
        self.requestHead = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Cookie": "st_si=33930137927479; FundWebTradeUserInfo=JTdCJTIyQ3VzdG9tZXJObyUyMjolMjIlMjIsJTIyQ3VzdG9tZXJOYW1lJTIyOiUyMiUyMiwlMjJWaXBMZXZlbCUyMjolMjIlMjIsJTIyTFRva2VuJTIyOiUyMiUyMiwlMjJJc1Zpc2l0b3IlMjI6JTIyJTIyLCUyMlJpc2slMjI6JTIyJTIyJTdE; qgqp_b_id=07e364420df0ba22c7e879b4c301457d; EMFUND1=null; EMFUND2=null; EMFUND3=null; EMFUND4=null; EMFUND5=null; EMFUND6=null; EMFUND7=null; EMFUND8=null; EMFUND0=null; EMFUND9=05-15 16:21:41@#$%u957F%u57CE%u4E2D%u503A1-3%u5E74%u653F%u91D1%u503AA@%23%24008652; st_asi=delete; st_pvi=10101638801112; st_sp=2022-05-15%2015%3A58%3A45; st_inirUrl=http%3A%2F%2Ffund.eastmoney.com%2Fdata%2F; st_sn=37; st_psi=20220515172651988-113300300999-4804406827; JSESSIONID=55F29FD72541EE153DAF30E7F0F13091",
        "Host": "datacenter-web.eastmoney.com",
        "Referer": "https://data.eastmoney.com/",
        "Sec-Fetch-Dest": "script",
        "Sec-Fetch-Mode": "no-cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
        "sec-ch-ua": "Not A;Brand ;v=99, Chromium;v=100, Google Chrome;v=100",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "macOS",
        }
        

    def _formateDailyURL(self, date,tableName,pageSize = 500):
        url = f'''https://datacenter-web.eastmoney.com/api/data/v1/get?callback={tableName}&sortColumns=SECURITY_CODE%2CTRADE_DATE&sortTypes=1%2C-1&pageSize={pageSize}&pageNumber=1&reportName=RPT_DAILYBILLBOARD_DETAILS&columns=SECURITY_CODE%2CSECUCODE%2CSECURITY_NAME_ABBR%2CTRADE_DATE%2CEXPLAIN%2CCLOSE_PRICE%2CCHANGE_RATE%2CBILLBOARD_NET_AMT%2CBILLBOARD_BUY_AMT%2CBILLBOARD_SELL_AMT%2CBILLBOARD_DEAL_AMT%2CACCUM_AMOUNT%2CDEAL_NET_RATIO%2CDEAL_AMOUNT_RATIO%2CTURNOVERRATE%2CFREE_MARKET_CAP%2CEXPLANATION%2CD1_CLOSE_ADJCHRATE%2CD2_CLOSE_ADJCHRATE%2CD5_CLOSE_ADJCHRATE%2CD10_CLOSE_ADJCHRATE&source=WEB&client=WEB&filter=(TRADE_DATE%3C%3D%27{date}%27)(TRADE_DATE%3E%3D%27{date}%27)'''
        return url


    def _formatBuyURL(self,date,tableName,stockID,pageSize = 50):
        url = f'''https://datacenter-web.eastmoney.com/api/data/v1/get?callback={tableName}&reportName=RPT_BILLBOARD_DAILYDETAILSBUY&columns=ALL&filter=(TRADE_DATE%3D%27{date}%27)(SECURITY_CODE%3D%22{stockID}%22)&pageNumber=1&pageSize={pageSize}&sortTypes=-1&sortColumns=BUY&source=WEB&client=WEB&_=1652606811700'''
        return url

    def _formatSELLURL(self,date,tableName,stockID,pageSize = 50):
        url = f'''https://datacenter-web.eastmoney.com/api/data/v1/get?callback={tableName}&reportName=RPT_BILLBOARD_DAILYDETAILSSELL&columns=ALL&filter=(TRADE_DATE%3D%27{date}%27)(SECURITY_CODE%3D%22{stockID}%22)&pageNumber=1&pageSize={pageSize}&sortTypes=-1&sortColumns=BUY&source=WEB&client=WEB&_=1652606811700'''
        return url


    def _baseRequest(self,tableName, url):
        response = requests.request("GET",url,headers=self.requestHead)
        print(response.status_code)
        data = response.text[response.text.find(tableName)+len(tableName)+1:-2]
        print(data)
        return json.loads(data)
    

    def FetchDailyData(self,date):
        tableName = "jQuery1123040640807664934187_1652615619385"
        url = self.formateDailyURL(date,tableName)
        j = self._baseRequest(tableName,url)
        #print(json.dumps(j,sort_keys=True,indent=4,separators=(",",": ")))
        reasons = [
        '连续三个交易日内，跌幅偏离值累计达到12%的ST证券、*ST证券和未完成股改证券', 
        '退市整理的证券',
        '退市整理期', 
        '连续三个交易日内，跌幅偏离值累计达到12%的ST证券、*ST证券', 
        'ST、*ST和S证券连续三个交易日内收盘价格跌幅偏离值累计达到15%的证券',
        ]
        stockIDs = [k["SECUCODE"] for k in j["result"]["data"] if k["EXPLANATION"] not in reasons]
        print(sorted(list(set(stockIDs))))
    

    def FetchByStockID(self,stockID,date):
        tableNameBuy = "jQuery112305166739778248102_1652606811698"
        tableNameSell = "jQuery112305166739778248102_1652606811696"
        buyUrl = self._formatBuyURL(date,tableNameBuy,stockID)
        sellUrl = self._formatSELLURL(date,tableNameSell,stockID)
        print(buyUrl)
        print(sellUrl)

        buyRes = self._baseRequest(tableNameBuy,buyUrl)
        sellRes = self._baseRequest(tableNameSell,sellUrl)
        self.ParserResult(buyRes)
        self.ParserResult(sellRes)


    def ParserResult(self,result):
        results = []
        datas = result["result"]["data"]
        for data in datas:
            stockID = data["SECUCODE"]
            operID = data["OPERATEDEPT_CODE"]
            operName = data["OPERATEDEPT_NAME"]
            reason = data["EXPLANATION"]
            b = data["BUY"]
            s = data["SELL"]
            n = data["NET"]
            print(operID, operName,reason,b,s,n)


if __name__ == "__main__":
    f = CDragonFetcher()
    #f.FetchDailyData("2022-05-13")
    f.FetchByStockID("000736","2022-05-13")