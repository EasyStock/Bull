import requests
import pandas as pd
import json



def Test():
    head = {
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

    #url='https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery112305166739778248102_1652606811698'
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery112305166739778248102_1652606811698&reportName=RPT_BILLBOARD_DAILYDETAILSBUY&columns=ALL&filter=(TRADE_DATE%3D%272022-03-09%27)(SECURITY_CODE%3D%22001313%22)&pageNumber=1&pageSize=50&sortTypes=-1&sortColumns=BUY&source=WEB&client=WEB&_=1652606811700"
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery112305166739778248102_1652606811698&reportName=RPT_BILLBOARD_DAILYDETAILSBUY&columns=ALL&filter=(TRADE_DATE%3D%272022-05-13%27)(SECURITY_CODE%3D%22001313%22)&pageNumber=1&pageSize=50&sortTypes=-1&sortColumns=BUY&source=WEB&client=WEB&_=1652606811700"
    # payload = {
    #     #"callback" : "jQuery112305166739778248102_1652606811698",
    #     "reportName" : "RPT_BILLBOARD_DAILYDETAILSBUY",
    #     "columns" : "ALL",
    #     "filter" : '''(TRADE_DATE='2022-03-09')(SECURITY_CODE="001313")''',
    #     "pageNumber" : 1,
    #     "pageSize" : 50,
    #     "sortTypes" : -1,
    #     "sortColumns" : "BUY",
    #     "source" : "WEB",
    #     "client" : "WEB",
    #     "_" : "1652606811700",
    # }
    #response = requests.request("GET",url,headers=head, data= json.dumps(payload))
    response = requests.request("GET",url,headers=head)
    print(response.status_code,response.text)



def Test2():
    head = {
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

    url = "https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery112305166739778248102_1652606811696&reportName=RPT_BILLBOARD_DAILYDETAILSSELL&columns=ALL&filter=(TRADE_DATE%3D%272022-03-09%27)(SECURITY_CODE%3D%22001313%22)&pageNumber=1&pageSize=50&sortTypes=-1&sortColumns=SELL&source=WEB&client=WEB&_=1652606811701"
    # payload = {
    #     #"callback" : "jQuery112305166739778248102_1652606811698",
    #     "reportName" : "RPT_BILLBOARD_DAILYDETAILSBUY",
    #     "columns" : "ALL",
    #     "filter" : '''(TRADE_DATE='2022-03-09')(SECURITY_CODE="001313")''',
    #     "pageNumber" : 1,
    #     "pageSize" : 50,
    #     "sortTypes" : -1,
    #     "sortColumns" : "BUY",
    #     "source" : "WEB",
    #     "client" : "WEB",
    #     "_" : "1652606811700",
    # }
    #response = requests.request("GET",url,headers=head, data= json.dumps(payload))
    response = requests.request("GET",url,headers=head)
    print(response.status_code,response.text)


def Test3():
    head = {
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

    url = "https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery112305166739778248102_1652606811698&reportName=RPT_BILLBOARD_DAILYDETAILS&columns=ALL&filter=(TRADE_DATE%3D%272022-03-09%27)(SECURITY_CODE%3D%22001313%22)&pageNumber=1&pageSize=10&sortTypes=&sortColumns=&source=WEB&client=WEB&_=1652606811702"
    # payload = {
    #     #"callback" : "jQuery112305166739778248102_1652606811698",
    #     "reportName" : "RPT_BILLBOARD_DAILYDETAILSBUY",
    #     "columns" : "ALL",
    #     "filter" : '''(TRADE_DATE='2022-03-09')(SECURITY_CODE="001313")''',
    #     "pageNumber" : 1,
    #     "pageSize" : 50,
    #     "sortTypes" : -1,
    #     "sortColumns" : "BUY",
    #     "source" : "WEB",
    #     "client" : "WEB",
    #     "_" : "1652606811700",
    # }
    #response = requests.request("GET",url,headers=head, data= json.dumps(payload))
    response = requests.request("GET",url,headers=head)
    print(response.status_code,response.text)

def Test4():
    head = {
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

    url = "https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery112307666070867155987_1652610892125&sortColumns=TRADE_DATE%2CSECURITY_CODE&sortTypes=-1%2C1&pageSize=50&pageNumber=1&reportName=RPT_OPERATEDEPT_TRADE_DETAILS&columns=ALL&filter=(OPERATEDEPT_CODE%3D%2210656871%22)&source=WEB&client=WEB"
    # payload = {
    #     #"callback" : "jQuery112305166739778248102_1652606811698",
    #     "reportName" : "RPT_BILLBOARD_DAILYDETAILSBUY",
    #     "columns" : "ALL",
    #     "filter" : '''(TRADE_DATE='2022-03-09')(SECURITY_CODE="001313")''',
    #     "pageNumber" : 1,
    #     "pageSize" : 50,
    #     "sortTypes" : -1,
    #     "sortColumns" : "BUY",
    #     "source" : "WEB",
    #     "client" : "WEB",
    #     "_" : "1652606811700",
    # }
    #response = requests.request("GET",url,headers=head, data= json.dumps(payload))
    response = requests.request("GET",url,headers=head)
    print(response.status_code,response.text)



def Test5():
    head = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        #"Cookie": "st_si=33930137927479; FundWebTradeUserInfo=JTdCJTIyQ3VzdG9tZXJObyUyMjolMjIlMjIsJTIyQ3VzdG9tZXJOYW1lJTIyOiUyMiUyMiwlMjJWaXBMZXZlbCUyMjolMjIlMjIsJTIyTFRva2VuJTIyOiUyMiUyMiwlMjJJc1Zpc2l0b3IlMjI6JTIyJTIyLCUyMlJpc2slMjI6JTIyJTIyJTdE; qgqp_b_id=07e364420df0ba22c7e879b4c301457d; EMFUND1=null; EMFUND2=null; EMFUND3=null; EMFUND4=null; EMFUND5=null; EMFUND6=null; EMFUND7=null; EMFUND8=null; EMFUND0=null; EMFUND9=05-15 16:21:41@#$%u957F%u57CE%u4E2D%u503A1-3%u5E74%u653F%u91D1%u503AA@%23%24008652; st_asi=delete; st_pvi=10101638801112; st_sp=2022-05-15%2015%3A58%3A45; st_inirUrl=http%3A%2F%2Ffund.eastmoney.com%2Fdata%2F; st_sn=37; st_psi=20220515172651988-113300300999-4804406827; JSESSIONID=55F29FD72541EE153DAF30E7F0F13091",
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

    url = "https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery1123040640807664934187_1652615619385&sortColumns=SECURITY_CODE%2CTRADE_DATE&sortTypes=1%2C-1&pageSize=500&pageNumber=1&reportName=RPT_DAILYBILLBOARD_DETAILS&columns=SECURITY_CODE%2CSECUCODE%2CSECURITY_NAME_ABBR%2CTRADE_DATE%2CEXPLAIN%2CCLOSE_PRICE%2CCHANGE_RATE%2CBILLBOARD_NET_AMT%2CBILLBOARD_BUY_AMT%2CBILLBOARD_SELL_AMT%2CBILLBOARD_DEAL_AMT%2CACCUM_AMOUNT%2CDEAL_NET_RATIO%2CDEAL_AMOUNT_RATIO%2CTURNOVERRATE%2CFREE_MARKET_CAP%2CEXPLANATION%2CD1_CLOSE_ADJCHRATE%2CD2_CLOSE_ADJCHRATE%2CD5_CLOSE_ADJCHRATE%2CD10_CLOSE_ADJCHRATE&source=WEB&client=WEB&filter=(TRADE_DATE%3C%3D%272022-05-13%27)(TRADE_DATE%3E%3D%272022-05-13%27)"
    # payload = {
    #     #"callback" : "jQuery112305166739778248102_1652606811698",
    #     "reportName" : "RPT_BILLBOARD_DAILYDETAILSBUY",
    #     "columns" : "ALL",
    #     "filter" : '''(TRADE_DATE='2022-03-09')(SECURITY_CODE="001313")''',
    #     "pageNumber" : 1,
    #     "pageSize" : 50,
    #     "sortTypes" : -1,
    #     "sortColumns" : "BUY",
    #     "source" : "WEB",
    #     "client" : "WEB",
    #     "_" : "1652606811700",
    # }
    #response = requests.request("GET",url,headers=head, data= json.dumps(payload))
    response = requests.request("GET",url,headers=head)
    print(response.status_code,response.text)

if __name__ == "__main__":
    Test()