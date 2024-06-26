
from thsData.fetchDataFromTHS import CFetchDataFromTHS
import re
import pandas as pd
import time
import datetime
import logging
import os
from workspace import workSpaceRoot,WorkSpaceFont,GetStockFolder
logger = logging.getLogger()

ZHANGTING_COLUMNS_MAP= {
    '股票代码' : '^股票代码',
    '股票简称' :'^股票简称',
    '上市天数':"^上市天数(天)<br>",
}

      
class CFetchDaLiangFromTHS(object):
    #今天成交量放大
    def __init__(self,cookie,v):
        self.dataFrame = None
        self.date = None
        self.keyWords = '今日成交量是昨日成交量的2.5倍以上 今日成家量是前一日成交量的2.5倍以上 今日成交量是5日平均成交量的2倍以上 非st 非退市,上市天数'
        self.url = 'http://x.iwencai.com/stockpick/search?ts=1&f=1&qs=stockhome_topbar_click&w=%E4%BB%8A%E6%97%A5%E6%88%90%E4%BA%A4%E9%87%8F%E6%98%AF%E6%98%A8%E6%97%A5%E6%88%90%E4%BA%A4%E9%87%8F%E7%9A%842.5%E5%80%8D%E4%BB%A5%E4%B8%8A%20%E4%BB%8A%E6%97%A5%E6%88%90%E5%AE%B6%E9%87%8F%E6%98%AF%E5%89%8D%E4%B8%80%E6%97%A5%E6%88%90%E4%BA%A4%E9%87%8F%E7%9A%842.5%E5%80%8D%E4%BB%A5%E4%B8%8A%20%E4%BB%8A%E6%97%A5%E6%88%90%E4%BA%A4%E9%87%8F%E6%98%AF5%E6%97%A5%E5%B9%B3%E5%9D%87%E6%88%90%E4%BA%A4%E9%87%8F%E7%9A%842%E5%80%8D%E4%BB%A5%E4%B8%8A%20%E9%9D%9Est%20%E9%9D%9E%E9%80%80%E5%B8%82,%E4%B8%8A%E5%B8%82%E5%A4%A9%E6%95%B0'
        self.referer = 'http://x.iwencai.com/stockpick/search?typed=1&preParams=&ts=1&f=1&qs=result_rewrite&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=%E4%BB%8A%E6%97%A5%E6%88%90%E4%BA%A4%E9%87%8F%E6%98%AF%E6%98%A8%E6%97%A5%E6%88%90%E4%BA%A4%E9%87%8F%E7%9A%842.5%E5%80%8D%E4%BB%A5%E4%B8%8A%20%E4%BB%8A%E6%97%A5%E6%88%90%E5%AE%B6%E9%87%8F%E6%98%AF%E5%89%8D%E4%B8%80%E6%97%A5%E6%88%90%E4%BA%A4%E9%87%8F%E7%9A%842.5%E5%80%8D%E4%BB%A5%E4%B8%8A%20%E4%BB%8A%E6%97%A5%E6%88%90%E4%BA%A4%E9%87%8F%E6%98%AF5%E6%97%A5%E5%B9%B3%E5%9D%87%E6%88%90%E4%BA%A4%E9%87%8F%E7%9A%842%E5%80%8D%E4%BB%A5%E4%B8%8A%20%E9%9D%9Est%20%E9%9D%9E%E9%80%80%E5%B8%82&queryarea='
        self.cookie = cookie
        self.v = v
        self.zhangTingDF = None
        self.Reasons = {}
        
    def GetDaLiangData(self):
        fetcher = CFetchDataFromTHS(self.cookie,self.url, self.referer, self.v)
        result = fetcher.FetchAllInOne_rawData()

        newData = [[data[0],data[1],data[2],data[-5],data[-4]] for data in  result[0]]
        newColumn = [result[1][0],result[1][1],result[1][2],result[1][-5],result[1][-4]]
        result = pd.DataFrame(newData,columns = newColumn)

        map = self.keywordTranslator(result)
        #print(map)
        self.dataFrame = pd.DataFrame()
        for key in map:
            self.dataFrame[key] = result[map[key]]
        
        # logger.info(str(self.dataFrame))
        # logger.info(f'{self.dataFrame[:10]}\n{self.dataFrame[-10:]}')
        # logger.info(self.date)
        # logger.info(f'{self.dataFrame.columns}')
        # logger.info(f'{self.dataFrame.shape}')
        folder = GetStockFolder(self.date)
        fileName = f'''大量_{self.date}'''
        self.DataFrameToJPG(self.dataFrame,["股票代码","股票简称"],folder,fileName)
        return self.dataFrame
    
    def _parserDate(self,key):
        k = '上市天数(天)<br>'
        if key.find(k) != -1:
            year,month,day = key[key.find(k)+len(k):].split('.')
            self.date = '%s-%s-%s'%(year,month,day)
        
    def keywordTranslator(self,dataframe):
        columnsKeys = ZHANGTING_COLUMNS_MAP.keys()
        dfKeys = dataframe.columns
        retMap = {}
        for key in columnsKeys:
            value = ZHANGTING_COLUMNS_MAP[key]
            for dfKey in dfKeys:
                if isinstance(dfKey,str) == False:
                    continue 
                if re.match(value, dfKey) != None:
                    retMap[key] = dfKey
                    
                if self.date is None:
                    self._parserDate(dfKey)
                
        return retMap


    def ConvertDataFrameToJPG(self,df,fullPath):
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

    def DataFrameToJPG(self,df,columns,rootPath, fileName):
        size = df.shape[0]
        step = 80
        if size > step:
            for index in range(0,size,step):
                tmp = df.iloc[index:,]
                if index + step <= size:
                    tmp = df.iloc[index:index+step,]
                fullPath = f"{rootPath}{fileName}_{int(index/step+1)}.jpg"
                print(fullPath)
                jpgDataFrame = pd.DataFrame(tmp,columns=columns)
                self.ConvertDataFrameToJPG(jpgDataFrame,fullPath)
        else:
            fullPath = f"{rootPath}{fileName}.jpg"
            print(fullPath)
            jpgDataFrame = pd.DataFrame(df,columns=columns)
            self.ConvertDataFrameToJPG(jpgDataFrame,fullPath)
