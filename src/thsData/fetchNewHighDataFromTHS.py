
from itertools import count
from operator import le
from tracemalloc import start

from pyparsing import col
from thsData.fetchDataFromTHS import CFetchDataFromTHS
import re
import pandas as pd
import logging
import datetime
import json
import os
from workspace import workSpaceRoot,WorkSpaceFont,GetStockFolder
logger = logging.getLogger()

NEWHIGH_COLUMNS_MAP= {
    '股票代码' : '^股票代码',
    '股票简称' :'^股票简称',
    '上市天数':"^上市天数",
    '所属概念':"^所属概念$",
    '所属概念数量':"^所属概念数量",
}

class CNewHighDataFromTHS(object):
    def __init__(self,cookie,v):
        self.dataFrame = None
        self.date = None
        self.keyWords = '创历史新高 上市天数大于200 非st 非退市,所属概念'
        self.url = 'http://x.iwencai.com/stockpick/search?typed=1&preParams=&ts=1&f=1&qs=result_rewrite&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=%E5%88%9B%E5%8E%86%E5%8F%B2%E6%96%B0%E9%AB%98%20%E4%B8%8A%E5%B8%82%E5%A4%A9%E6%95%B0%E5%A4%A7%E4%BA%8E200%20%E9%9D%9Est%20%E9%9D%9E%E9%80%80%E5%B8%82%2C%E6%89%80%E5%B1%9E%E6%A6%82%E5%BF%B5&queryarea='
        self.referer = 'http://x.iwencai.com/stockpick/search?typed=1&preParams=&ts=1&f=1&qs=result_rewrite&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=%E6%98%A8%E6%97%A5%E9%A6%96%E6%9D%BF%EF%BC%8C%E9%9D%9Est%EF%BC%8C%E9%9D%9E%E6%96%B0%E8%82%A1%EF%BC%8C%E9%9D%9E%E7%A7%91%E5%88%9B%E6%9D%BF%EF%BC%8C%E9%9D%9E%E5%88%9B%E4%B8%9A%E6%9D%BF&queryarea='
        self.cookie = cookie
        self.v = v
        self.newHighDF = None
        self.Reasons = {}
        
    def GetNewHighData(self):
        fetcher = CFetchDataFromTHS(self.cookie,self.url, self.referer, self.v)
        result = fetcher.FetchAllInOne()
        map = self.keywordTranslator(result)
        self.dataFrame = pd.DataFrame()
        for key in map:
            self.dataFrame[key] = result[map[key]]
        
        self.ParserGaiNian()
        logger.info(f'{self.dataFrame}')
        rootFolder = GetStockFolder(self.date)

        fullPath = f"{rootFolder}新高_{self.date}.jpg"
        jpgDataFrame = pd.DataFrame(self.dataFrame,columns=["股票代码","股票简称"])
        self.ConvertDataFrameToJPG(jpgDataFrame,fullPath)

    
    def ConvertDataFrameToJPG(self, df,fullPath):
        if df.empty:
            return
        from pandas.plotting import table
        import matplotlib.pyplot as plt
        plt.rcParams["font.sans-serif"] = [WorkSpaceFont]#显示中文字体
        high = int(0.174 * df.shape[0]+0.5)+1
        fig = plt.figure(figsize=(3, high), dpi=200)#dpi表示清晰度
        ax = fig.add_subplot(111, frame_on=False) 
        ax.xaxis.set_visible(False)  # hide the x axis
        ax.yaxis.set_visible(False)  # hide the y axis
        table(ax, df, loc='center')  # 将df换成需要保存的dataframe即可
        plt.savefig(fullPath)
        plt.close()
        
    
    def _parserDate(self,key):
        k = '上市天数(天)<br>'
        if key.find(k) != -1:
            year,month,day = key[key.find(k)+len(k):].split('.')
            self.date = '%s-%s-%s'%(year,month,day)
        
    def keywordTranslator(self,dataframe):
        columnsKeys = NEWHIGH_COLUMNS_MAP.keys()
        dfKeys = dataframe.columns
        retMap = {}
        for key in columnsKeys:
            value = NEWHIGH_COLUMNS_MAP[key]
            for dfKey in dfKeys:
                if re.match(value, dfKey) != None:
                    retMap[key] = dfKey
                    
                if self.date is None:
                    self._parserDate(dfKey)
                
        return retMap


    def ParserGaiNian(self):
        if self.dataFrame is None:
            return
        
        gaiNian = {

        }
        exceptions = ["转融券标的","融资融券","深股通","沪股通","标普道琼斯A股","MSCI概念","富时罗素概念","富时罗素概念股","B转H",]
        for _,row in self.dataFrame.iterrows():
            stockName = row["股票简称"]
            stockID = row["股票代码"]
            fName = f"{stockID}_{stockName}"
            gaiNians = row["所属概念"].split(";")
            for g in gaiNians:
                if g in exceptions:
                    continue
                if g not in gaiNian:
                    gaiNian[g] = []
                
                gaiNian[g].append(fName)
        
        #print(json.dumps(gaiNian,sort_keys=True,indent=2,separators=(",",": "),ensure_ascii=False))
        t = sorted(gaiNian.items(),key=lambda x:len(x[1]),reverse=False)
        for k in t:
            s = ""
            step = 10
            count = int(len(k[1])/step)
            for index in range(count+1):
                start = index*step
                end = index*step + step
                if end > len(k[1]):
                    end = len(k[1])

                subInfo = k[1][start:end]
                s = s + "   ".join(subInfo) + "\n       "

            res = f'''{k[0]:15} {len(k[1]):5}
====================================
       {s}'''
            print(res)
