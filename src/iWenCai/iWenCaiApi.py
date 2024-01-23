import requests
import json
import re
import pandas as pd
import logging
import math
import time
from iWenCai.getHexinV import get_hexin_v

logger = logging.getLogger()

class CIWenCaiAPI(object):
    def __init__(self,cookie = None,dbConnection = None) :
        self.cookie = cookie
        self.dbConnection = dbConnection
        self.allDataCount = None
        self.moreUrl = None
        self._getCookieFromDB()

    def GetTHS_V(self):
        return get_hexin_v()
    
    def _getCookieFromDB(self):
        if self.cookie is None and self.dbConnection is not None:
            sql = f'''SELECT cookie FROM cookies where name = "iwencai";'''
            results, _ = self.dbConnection.Query(sql)
            self.cookie = results[0][0]
    
    def RequestFirstData(self,payload,url = "http://www.iwencai.com/customized/chart/get-robot-data"):
        d = json.dumps(payload)
        logger.warning(f'''准备下载的关键词是: {payload["question"]}''')
        contentLength = len(d)
        v = self.GetTHS_V()
        # v = "A5cWFl7svn8wWDrz0HMDQRfKIADk3GuPRbHvsunFsT3NBLn-8az7jlWAfx76"

        if self.cookie:
            #print(self.cookie)
            c = []
            all = self.cookie.split(";")
            for a in all:
                c.append(re.sub("v=[\s\S]*",f'''v={v}''',a))
            self.cookie = ";".join(c)
            #print(self.cookie)

        headers = {
        'Accept': 'application/json, text/plain, */*',  #
        'Accept-Encoding': 'gzip, deflate, br',  #
        'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7', #
        'Cache-Control': 'no-cache', #
        'Connection': 'keep-alive', #
        'Content-Type': 'application/json', #
        'Content-Length': str(contentLength), #
        'Host': 'www.iwencai.com',  #
        'Origin': 'http://www.iwencai.com', #
        'Pragma': 'no-cache', #
        'Referer': 'http://www.iwencai.com', #
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36', #
        "Hexin-V":v, 
        "Cookie":self.cookie,
        "Sec-Ch-Ua": "Not_A Brand;v=8, Google Chrome;v=120, Chromium;v=120",
        "Sec-Ch-Ua-Mobile":"?0",
        "Sec-Ch-Ua-Platform":"macOS",
        "Sec-Fetch-Dest":"empty",
        "Sec-Fetch-Mode":"cors",
        "Sec-Fetch-Site":"same-origin",
        }

        try:
            response = requests.request("POST",url,headers=headers, data= d)
            #print(response.text)
            js =json.loads(response.text)
            components = js['data']['answer'][0]["txt"][0]["content"]["components"][0]
            data = components["data"]
            self.moreUrl = components["config"]["other_info"]["footer_info"]["url"]
            self.allDataCount = data["meta"]["extra"]["row_count"]
        except Exception as e:
            print(e)

    
    def RequestOnePage(self,perPage,page):
        if self.moreUrl is None:
            return
        url,oldPayload = self.moreUrl.split("?")
        oldPayload = re.sub("&perpage=\d{1,}&",f'''&perpage={perPage}&''',oldPayload)
        newPayload = re.sub("&page=\d{1,}&",f'''&page={page}&''',oldPayload)
        contentLength = len(newPayload)

        v = self.GetTHS_V()
       #v = "A5cWFl7svn8wWDrz0HMDQRfKIADk3GuPRbHvsunFsT3NBLn-8az7jlWAfx76"

        if self.cookie:
            self.cookie = re.sub("v=?;",f'''v={v};''',self.cookie)

        headers = {
        'Accept': 'application/json, text/plain, */*', 
        'Accept-Encoding': 'gzip, deflate', 
        'Accept-Language': 'zh-CN,zh;q=0.9', 
        'Cache-Control': 'no-cache', 
        'Connection': 'keep-alive', 
        'Content-Type': 'application/x-www-form-urlencoded', 
        'Content-Length': str(contentLength),
        'Host': 'www.iwencai.com',  
        'Origin': 'http://www.iwencai.com', 
        'Pragma': 'no-cache', 
        'Referer': 'http://www.iwencai.com',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko)', #
        "hexin-v":v, 
        "Cookie":self.cookie
        }
        url = "http://www.iwencai.com"+url
        try:
            response = requests.request("POST", url, headers=headers, data=newPayload)
            js =json.loads(response.text)
            components = js["answer"]["components"]
            component = components[0]
            datas = component["data"]["datas"]
            return datas
        except:
            return self.RequestOnePage(perPage,page)

    def RequestOnePagereAndTry(self,perPage,page,totalPages,existingSize,totalSize,retryCount = 1):
        datas = self.RequestOnePage(perPage,page)
        size = len(datas)
        logger.error(f"一共有{totalSize:^5d}条数据,已经获取第 【{page:^5d}】页数据, 每页 【{perPage:^5d}】条, 本次总共获取了 【{size:^5d}】 条数据, 总共获取了【{existingSize+size:^8d}】 条数据, 第{retryCount}次尝试")
        if page != totalPages and size != perPage and retryCount <=5:
            retryCount = retryCount + 1
            return self.RequestOnePagereAndTry(perPage,page,totalPages,totalSize,retryCount)
        
        return datas

    def RequestAllPagesData(self,payload,perPage = 100):
        for i in range(1,6):
            self.RequestFirstData(payload)
            if self.allDataCount is None:
                time.sleep(3)
                continue
            else:
                break
            
        allDatas = []
        if self.allDataCount is not None:
            pages = math.ceil(self.allDataCount / perPage )
            for i in range(1,pages+1):
                totalSize = len(allDatas)
                time.sleep(1)
                datas = self.RequestOnePagereAndTry(perPage,i,pages,totalSize,self.allDataCount,1)
                allDatas.extend(datas)
            
        df = pd.DataFrame(allDatas)
        return df
        #print(df)