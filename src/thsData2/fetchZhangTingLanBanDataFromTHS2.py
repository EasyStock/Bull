import pandas as pd
import re
from thsData2.fetchDataFromTHS2 import CFetchDataFromTHS2
import datetime
import logging
import os


logger = logging.getLogger()

ZHANGTING_COLUMNS_MAP= {
    '股票代码' : '^股票代码',
    '股票简称' :'^股票简称',
    '涨停封单量(股)':"^涨停封单量",
    '涨停封单额(元)':"^涨停封单额",
    '涨停开板次数' : '^涨停开板次数',
    '涨停' : '^涨停\[',
}

class CFetchZhangTingLanBanDataFromTHS2(object):
    def __init__(self,date,v):
        self.dataFrame = None
        self.date = date
        self.v = v

    def RequestDailyData(self,page = 1,perPage = 5000):
            query = '非st 涨停打开次数大于等于1 非退市'
            Condition = '''[{"chunkedResult":"非st 涨停打开次数大于等于1 _&_非退市","opName":"and","opProperty":"","sonSize":4,"relatedSize":0},{"indexName":"股票简称","indexProperties":["不包含st,退"],"source":"new_parser","type":"index","indexPropertiesMap":{"不包含":"st,退"},"reportType":"null","valueType":"_股票简称","domain":"abs_股票领域","uiText":"股票简称不包含st","sonSize":0,"queryText":"股票简称不包含st","relatedSize":0,"tag":"股票简称"},{"opName":"and","opProperty":"","sonSize":2,"relatedSize":0},{"indexName":"涨停开板次数","indexProperties":["nodate 1","交易日期 20220909","(=1"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20220909","(=":"1","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_整型数值(次|个)","domain":"abs_股票领域","uiText":"涨停开板次数>=1次","sonSize":0,"queryText":"涨停开板次数>=1次","relatedSize":0,"tag":"涨停开板次数"},{"indexName":"股票简称","indexProperties":["不包含st,退"],"source":"new_parser","type":"index","indexPropertiesMap":{"不包含":"st,退"},"reportType":"null","valueType":"_股票简称","domain":"abs_股票领域","uiText":"股票简称不包含退","sonSize":0,"queryText":"股票简称不包含退","relatedSize":0,"tag":"股票简称"}]'''
            d = datetime.datetime.strptime(str(self.date), "%Y-%m-%d").date()
            newDate = d.strftime("%Y%m%d")

            Condition = Condition.replace("20220909",newDate)
            ths = CFetchDataFromTHS2(query,Condition)
            ths.page = page
            ths.perPage = perPage
            ths.dateRange0 = newDate
            ths.dateRange1 = newDate

            df = ths.RequstData(self.v)
            #print(df)
            map = self.keywordTranslator(df)
            tmpDataFrame = pd.DataFrame()
            for key in map:
                tmpDataFrame[key] = df[map[key]]

            #print(tmpDataFrame)
            # 去掉一封就封死的票
            yifengZhangTing = tmpDataFrame[((tmpDataFrame['涨停开板次数'] == 1) & (tmpDataFrame['涨停'] == "涨停")) ]
            #print(yifengZhangTing)
            self.dataFrame = tmpDataFrame[~((tmpDataFrame['涨停开板次数'] == 1) & (tmpDataFrame['涨停'] == "涨停")) ]
            self.dataFrame.reset_index(drop = True,inplace = True)
            #logger.info(str(self.dataFrame))
            #logger.info(f'{self.dataFrame[:10]}\n{self.dataFrame[-10:]}')
            # logger.info(self.date)
            # logger.info(f'{self.dataFrame.columns}')
            # logger.info(f'{self.dataFrame.shape}')
            folder = f'/Volumes/Data/复盘/股票1/{self.date}/'
            if os.path.exists(folder) == False:
                os.makedirs(folder)

            fileName = f'''烂板_{self.date}'''
            fileName1 = f'''一封涨停_{self.date}'''
            self.DataFrameToJPG(self.dataFrame,["股票代码","股票简称"],folder,fileName)
            self.DataFrameToJPG(yifengZhangTing,["股票代码","股票简称"],folder,fileName1)
            return self.dataFrame


    def keywordTranslator(self,dataframe):
        columnsKeys = ZHANGTING_COLUMNS_MAP.keys()
        dfKeys = dataframe.columns
        retMap = {}
        for key in columnsKeys:
            value = ZHANGTING_COLUMNS_MAP[key]
            for dfKey in dfKeys:
                if re.match(value, dfKey) != None:
                    retMap[key] = dfKey
           
        return retMap


    def ConvertDataFrameToJPG(self,df,fullPath):
        from pandas.plotting import table
        import matplotlib.pyplot as plt
        plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']#显示中文字体
        high = int(0.174 * df.shape[0]+0.5)+1
        fig = plt.figure(figsize=(3, high), dpi=200)#dpi表示清晰度
        ax = fig.add_subplot(111, frame_on=False) 
        ax.xaxis.set_visible(False)  # hide the x axis
        ax.yaxis.set_visible(False)  # hide the y axis
        table(ax, df, loc='center')  # 将df换成需要保存的dataframe即可
        plt.savefig(fullPath)

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
            if df.empty == True:
                return
            fullPath = f"{rootPath}{fileName}.jpg"
            print(fullPath)
            jpgDataFrame = pd.DataFrame(df,columns=columns)
            self.ConvertDataFrameToJPG(jpgDataFrame,fullPath)