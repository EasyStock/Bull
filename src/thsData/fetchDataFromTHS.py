import requests
import json
import pandas as pd
import logging

logger = logging.getLogger()
class CFetchDataFromTHS(object):
    def __init__(self,cookie,url,referer,v):
        self.cookie =cookie
        self.url = url
        self.referer = referer
        self.v = v
        self.token = None
        self.dataFrame = None
        
    def Search(self):
        requestsCookie = self.cookie+"v="+self.v
        request_head = {
        "Host": "x.iwencai.com",
        "Cache-Control":"max-age=0",
        "Upgrade-Insecure-Requests":"1",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cookie": requestsCookie,
        "Connection": "keep-alive",
        }
        
        response = requests.get(url=self.url,headers=request_head)
        start = response.text.find('/*将问句搜索返回的json数据传入到JS中*/') + len('/*将问句搜索返回的json数据传入到JS中*/')
        end = response.text.find('// 是否出添加概念按钮')
        r = response.text[start:end]
        r = r.strip()
        r = r[len('var allResult = '):-1]
        js =json.loads(r)
        self.token = js['token']
        logger.info(f"get token:{self.token}")
    
    def GetCache(self):
        requestsCookie = self.cookie+"v="+self.v
        sse_head = {
                "Host": "x.iwencai.com",
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
                "hexin-v": self.v,
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
                "Referer": self.referer,
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
                "Cookie": requestsCookie,
                "Connection": "keep-alive",
                }
        requestURL = 'http://x.iwencai.com/stockpick/cache?token='+ self.token +'&p=1&perpage=6000&changeperpage=1&showType=[%22%22,%22%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22]'
        response = requests.get(url=requestURL,headers=sse_head)
        # print(response.text)
        data = json.loads(response.text)
        self.dataFrame = pd.DataFrame(data['result'],columns = data['title'])
        logger.info(f'{self.dataFrame.columns}')
        return self.dataFrame

    def FetchAllInOne(self):
        self.Search()
        return self.GetCache()