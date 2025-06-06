#coding=utf-8
from urllib.parse import quote
from xml.dom.expatbuilder import theDOMImplementation
import requests
import json
import pandas as pd
import logging
import time

logger = logging.getLogger()

class CFetchDataFromTHS_MultiPageMgr(object):
    def __init__(self,query,condition) -> None:
        self.query = query
        self.urp_sort_way = "asc"
        self.urp_sort_index = '股票代码'
        self.condition = condition
        self.codelist = ""
        self.indexNameLimit = ""
        self.logid = "046fe0b4300450398d4944ff01ece60e"
        self.ret = 'json_all'
        self.sessignid = "046fe0b4300450398d4944ff01ece60e"
        self.dateRange0 = "20220902"
        self.dateRange1 = "20220902"
        self.iwc_token = "0ac9665417016889621435948"
        self.user_id = "240679370"
        self.uuids = 24087
        self.query_type = 'stock'
        self.comp_id = '6836372'
        self.business_cat = 'soniu'
        self.uuid = 24087
        self.urp_use_sort =1

        self.queryString = None
        self.queryLength = 0
        self.dataFrame = None
        self.threshold = -1
    
    def RequestOnePage(self,v,page,perPage):
        ths = CFetchDataFromTHS2(self.query,self.condition)
        ths.page = page
        ths.perPage = perPage

        ths.query = self.query
        ths.urp_sort_way = self.urp_sort_way
        ths.urp_sort_index = self.urp_sort_index
        ths.condition = self.condition
        ths.codelist = self.codelist
        ths.indexNameLimit = self.indexNameLimit
        ths.logid = self.logid
        ths.ret = self.ret
        ths.sessignid = self.sessignid
        ths.dateRange0 = self.dateRange0
        ths.dateRange1 = self.dateRange1
        ths.iwc_token = self.iwc_token
        ths.user_id = self.user_id
        ths.uuids = self.uuids
        ths.query_type = self.query_type
        ths.comp_id = self.comp_id
        ths.business_cat = self.business_cat
        ths.uuid = self.uuid
        ths.urp_use_sort =self.urp_use_sort
        df = ths.RequstData(v)
        return df

    def RequestOnePageAndAppendData(self,v,page,perPage):
            tmpData = self.RequestOnePage(v,page,perPage)
            if self.dataFrame is None:
                self.dataFrame = tmpData
            else:
                self.dataFrame = pd.concat([self.dataFrame,tmpData],ignore_index=True)

            threshold = self.dataFrame.shape[0]
            currentCount = tmpData.shape[0]
            logger.error(f"开始获取第 【{page:^5d}】页数据, 每页 【{perPage:^5d}】条, 本次总共获取了 【{currentCount:^5d}】 条数据, 总共获取了【{threshold:^8d}】 条数据")
            return tmpData

    def RequestMutiPageData(self,v,perPage = 100):
        logger.warning(f'''查询数据条件: 【 {self.query} 】''')
        zeroCount = 0
        for pageID in range(1,50000):
            tmpData = self.RequestOnePageAndAppendData(v,pageID,perPage)
            currentCount = tmpData.shape[0]
            if zeroCount >=5:
                break

            if currentCount == 0:
                logger.warning(f'''重新获取 第【{pageID}】页数据 查询数据条件: 【 {self.query} 】''')
                self.RequestOnePageAndAppendData(v,pageID,perPage)
                zeroCount = zeroCount +1
                continue

            threshold = self.dataFrame.shape[0]
            if currentCount < perPage and threshold >= self.threshold:
                break
            time.sleep(3)
        
        if self.dataFrame.empty:
            return None

        logger.warning(f"总共获取了 [{pageID}] 页数据,总共 [{self.dataFrame.shape[0]}] 条")

        return self.dataFrame

class CFetchDataFromTHS2(object):
    def __init__(self,query,condition) -> None:
        self.query = query
        self.urp_sort_way = "asc"
        self.urp_sort_index = '股票代码'
        self.page = 1
        self.perPage = 50
        self.condition = condition
        self.codelist = ""
        self.indexNameLimit = ""
        self.logid = "10baacb6322e6783d37d3d12e0d0d6291"
        self.ret = 'json_all'
        self.sessignid = "10baacb6322e6783d37d3d12e0d0d6291"
        self.dateRange0 = "20220902"
        self.dateRange1 = "20220902"
        self.iwc_token = "0ac9665417016889621435948"
        self.user_id = "240679370"
        self.uuids = 24087
        self.query_type = 'stock'
        self.comp_id = '6836372'
        self.business_cat = 'soniu'
        self.uuid = 24087
        self.urp_use_sort =1

        self.queryString = None
        self.queryLength = 0
       
    def QueryHead(self,v):
        headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': str(self.queryLength),
        'Host': 'ai.iwencai.com',  
        'Origin': 'http://www.iwencai.com',
        'Pragma': 'no-cache',
        'Referer': 'http://www.iwencai.com',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko)',
        "hexin-v":v,
        "Cookie": "other_uid=Ths_iwencai_Xuangu_1drlkslkp3tsos9aqg6jqww8t35ngijw; ta_random_userid=9lpymr2sgu; cid=d47f21233341e678c876d879694df0801746533872; u_ukey=A10702B8689642C6BE607730E11E6E4A; u_uver=1.0.0; u_dpass=QEUvYONrurEqt1CbfhRaz%2BfXDGkxudI%2BsXOE%2BmpwF0iSR9GvU9ALtcQ3lSQVRQvVHi80LrSsTFH9a%2B6rtRvqGg%3D%3D; u_did=C445ABCC484249B5B20EAA41373D9408; u_ttype=WEB; ttype=WEB; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNzQ2NTM0NDg4Ojo6MTQzMDEzNjkwMDo2MDQ4MDA6MDoxMTA4NDI0YzI5NWJmMGM1N2UwNGM3YTFkMzMwNTE2MmU6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=25e81d76ff814f9ab38ffcc21005a072; user_status=0; utk=c800111133b7f16573b981f001ee35f0; v=Ax5d9o7r-lBaYi43-H3fi7z9ab9l3-OEdKKWK8initNY5LBhMG8yaUQz5m2b"
        }
        return headers


    def formatQueryString(self):
        query_urlEncoded = quote(self.query)
        urp_sort_index_encode = quote(self.urp_sort_index)
        condition_encode = quote(self.condition)
        firstpart = f'''query={query_urlEncoded}&urp_sort_way={self.urp_sort_way}&urp_sort_index={urp_sort_index_encode}&page={self.page}&perpage={self.perPage}&condition={condition_encode}'''
        secondpart = f'''codelist={self.codelist}&indexnamelimit={self.indexNameLimit}&logid={self.logid}&ret={self.ret}&sessionid={self.sessignid}&date_range%5B0%5D={self.dateRange0}&date_range%5B1%5D={self.dateRange1}&iwc_token={self.iwc_token}&urp_use_sort={self.urp_use_sort}&user_id={self.user_id}'''
        thirdPart = f'''uuids%5B0%5D={self.uuids}&query_type={self.query_type}&comp_id={self.comp_id}&business_cat={self.business_cat}&uuid={self.uuid}'''
        self.queryString = f'''{firstpart}&{secondpart}&{thirdPart}'''
        self.queryLength = len(self.queryString)
        # print(self.queryString)
        # print(self.queryLength)
        return self.queryString


    def RequstData(self,v):
        #url = "https://ai.iwencai.com/urp/v7/landing/getDataList"
        #url = "http://www.iwencai.com/gateway/urp/v7/landing/getDataList?hexin-v={v}"
        #url = "https://ai.iwencai.com/urp/v7/landing/getDataList?hexin-v={v}"
        url = "http://www.iwencai.com/gateway/urp/v7/landing/getDataList"
        payload = self.formatQueryString()
        headers = self.QueryHead(v)
        try:
            response = requests.request("POST", url, headers=headers, data=payload)
            js =json.loads(response.text)
            components = js["answer"]["components"]
            component = components[0]
            datas = component["data"]["datas"]
            df = pd.DataFrame(datas)
            return df
        except:
            return self.RequstData(v)
        #logger.error(f"共获取{df.shape[0]}条数据")
        


 
if __name__ == "__main__":
    query = '创历史新高 上市天数大于200 非st 非退市,所属概念'
    Condition = '''[{"chunkedResult":"创历史新高 _&_上市天数大于200 _&_非st _&_非退市,_&_所属概念","opName":"and","opProperty":"","sonSize":8,"relatedSize":0},{"indexName":"股价创历史新高","indexProperties":["nodate 1","交易日期 20220909"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20220909","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_是否","domain":"abs_股票领域","uiText":"股价创历史新高","sonSize":0,"queryText":"股价创历史新高","relatedSize":0,"tag":"股价创历史新高"},{"opName":"and","opProperty":"","sonSize":6,"relatedSize":0},{"indexName":"上市天数","indexProperties":["nodate 1","交易日期 20220909","(200"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20220909","(":"200","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_整型数值(天)","domain":"abs_股票领域","uiText":"上市天数>200天","sonSize":0,"queryText":"上市天数>200天","relatedSize":0,"tag":"上市天数"},{"opName":"and","opProperty":"","sonSize":4,"relatedSize":0},{"indexName":"股票简称","indexProperties":["不包含st,退"],"source":"new_parser","type":"index","indexPropertiesMap":{"不包含":"st,退"},"reportType":"null","valueType":"_股票简称","domain":"abs_股票领域","uiText":"股票简称不包含st","sonSize":0,"queryText":"股票简称不包含st","relatedSize":0,"tag":"股票简称"},{"opName":"and","opProperty":"","sonSize":2,"relatedSize":0},{"indexName":"股票简称","indexProperties":["不包含st,退"],"source":"new_parser","type":"index","indexPropertiesMap":{"不包含":"st,退"},"reportType":"null","valueType":"_股票简称","domain":"abs_股票领域","uiText":"股票简称不包含退","sonSize":0,"queryText":"股票简称不包含退","relatedSize":0,"tag":"股票简称"},{"indexName":"所属概念","indexProperties":[],"source":"new_parser","type":"index","indexPropertiesMap":{},"reportType":"null","valueType":"_所属概念","domain":"abs_股票领域","uiText":"所属概念","sonSize":0,"queryText":"所属概念","relatedSize":0,"tag":"所属概念"}]'''
    ths = CFetchDataFromTHS2(query,Condition)
    df = ths.RequstData("A1-cbY4HQBnleEQFv_Aujk746LjsxKa2zQa3CPGS-YtXsHGm-ZRDtt3oR-IC")
    print(df)
    t = json.loads(Condition)
    print(t)
   