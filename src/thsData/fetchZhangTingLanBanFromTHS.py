
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
    '涨停封单量(股)':"^涨停封单量",
    '涨停封单额(元)':"^涨停封单额",
    '涨停开板次数' : '^涨停开板次数',
    '涨停' : '^涨停<br>',
}

      
class CFetchZhangTingLanBanFromTHS(object):
    def __init__(self,cookie,v):
        # 涨停烂板数据
        self.dataFrame = None
        self.date = None
        self.keyWords = '非st 涨停打开次数大于等于1 非退市,涨停，涨停开板次数，涨停封单额'
        self.url = 'http://x.iwencai.com/stockpick/search?typed=1&preParams=&ts=1&f=1&qs=result_rewrite&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=%E9%9D%9Est%20%E6%B6%A8%E5%81%9C%E6%89%93%E5%BC%80%E6%AC%A1%E6%95%B0%E5%A4%A7%E4%BA%8E%E7%AD%89%E4%BA%8E1%20%E9%9D%9E%E9%80%80%E5%B8%82&queryarea='
        self.referer = 'http://x.iwencai.com/stockpick/search?ts=1&f=1&qs=stockhome_topbar_click&w=%E9%9D%9Est%20%E6%B6%A8%E5%81%9C%E6%89%93%E5%BC%80%E6%AC%A1%E6%95%B0%E7%AD%89%E4%BA%8E1%20%E9%9D%9E%E9%80%80%E5%B8%82,%E6%B6%A8%E5%81%9C%EF%BC%8C%E6%B6%A8%E5%81%9C%E5%BC%80%E6%9D%BF%E6%AC%A1%E6%95%B0%EF%BC%8C%E6%B6%A8%E5%81%9C%E5%B0%81%E5%8D%95%E9%A2%9D'
        self.cookie = cookie
        self.v = v
        self.zhangTingDF = None
        self.Reasons = {}
        
    def GetZhangTingLanBanData(self):
        fetcher = CFetchDataFromTHS(self.cookie,self.url, self.referer, self.v)
        result = fetcher.FetchAllInOne()
        #print(result)
        map = self.keywordTranslator(result)
        #print(map)
        tmpDataFrame = pd.DataFrame()
        for key in map:
            tmpDataFrame[key] = result[map[key]]
        
        #print(tmpDataFrame)
        # 去掉一封就封死的票
        print(tmpDataFrame[((tmpDataFrame['涨停开板次数'] == 1) & (tmpDataFrame['涨停'] == "涨停")) ])
        self.dataFrame = tmpDataFrame[~((tmpDataFrame['涨停开板次数'] == 1) & (tmpDataFrame['涨停'] == "涨停")) ]
        self.dataFrame.reset_index(drop = True,inplace = True)
        logger.info(str(self.dataFrame))
        #logger.info(f'{self.dataFrame[:10]}\n{self.dataFrame[-10:]}')
        logger.info(self.date)
        logger.info(f'{self.dataFrame.columns}')
        logger.info(f'{self.dataFrame.shape}')
        folder = GetStockFolder(self.date)
        
        fileName = f'''烂板_{self.date}'''
        self.DataFrameToJPG(self.dataFrame,["股票代码","股票简称"],folder,fileName)
        return self.dataFrame
    
    def _parserDate(self,key):
        k = '首次涨停时间<br>'
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
