import pandas as pd
import re
from thsData2.fetchDataFromTHS2 import CFetchDataFromTHS2,CFetchDataFromTHS_MultiPageMgr
import datetime
import logging
import os


logger = logging.getLogger()

ZHANGTING_COLUMNS_MAP= {
    '股票代码' : '^股票代码',
    '股票简称' :'^股票简称',
}

class CFetchDaliangDataFromTHS2(object):
    def __init__(self,dates,v):
        self.dataFrame = None
        self.dates = dates
        self.v = v

    def RequestDaliangData(self):
        step = 100
        for pageID in range(1,500):
            tmpData = self._RequestDaliangData(pageID,step)
            if self.dataFrame is None:
                self.dataFrame = tmpData
            else:
                self.dataFrame = pd.concat([self.dataFrame,tmpData],ignore_index=True)

            size = tmpData.shape[0]
            if size < step:
                break
        
        if self.dataFrame.empty:
            return

        logger.warning(f"获取大量数据 共 [{pageID}] 页,总共 [{self.dataFrame.shape[0]}] 条")

        folder = f'/Volumes/Data/复盘/股票_New/{self.dates[-1]}/'
        if os.path.exists(folder) == False:
            os.makedirs(folder)

        fileName = f'''大量_{self.dates[-1]}'''
        self.DataFrameToJPG(self.dataFrame,["股票代码","股票简称"],folder,fileName)
        return self.dataFrame


    def _RequestDaliangData(self,page = 1,perPage = 100):
            query = '今天AAA成交量是昨天BBB成交量的1.8倍以上 今天AAA成交量是前天CCC成交量的1.8倍以上 今天AAA成交量是5日平均成交量的1.8倍以上 非st 非退市'
            Condition = '''[{"chunkedResult":"今天AAA成交量是昨天BBB成交量的1.8倍以上 _&_今天AAA成交量是前天CCC成交量的1.8倍以上 _&_今天AAA成交量是5日平均成交量的1.8倍以上 _&_非st _&_非退市","opName":"and","opProperty":"","sonSize":14,"relatedSize":0},{"opName":"/","opProperty":"(=1.8","uiText":"（AAA今天的成交量/BBB昨天的成交量）>=1.8倍","sonSize":2,"queryText":"（AAA今天的成交量/BBB昨天的成交量）>=1.8倍","relatedSize":2},{"dateText":"今天AAA","indexName":"成交量","indexProperties":["交易日期 今天AAA"],"dateUnit":"日","source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"今天AAA"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_浮点型数值(股)","domain":"abs_股票领域","sonSize":0,"relatedSize":0,"tag":"[今天AAA]成交量"},{"dateText":"昨天BBB","indexName":"成交量","indexProperties":["交易日期 昨天BBB"],"dateUnit":"日","source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"昨天BBB"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_浮点型数值(股)","domain":"abs_股票领域","sonSize":0,"relatedSize":0,"tag":"[今天AAA]成交量"},{"opName":"and","opProperty":"","sonSize":10,"relatedSize":0},{"opName":"/","opProperty":"(=1.8","uiText":"（AAA今天的成交量/CCC前天的成交量）>=1.8倍","sonSize":2,"queryText":"（AAA今天的成交量/CCC前天的成交量）>=1.8倍","relatedSize":2},{"dateText":"今天AAA","indexName":"成交量","indexProperties":["交易日期 今天AAA"],"dateUnit":"日","source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"今天AAA"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_浮点型数值(股)","domain":"abs_股票领域","sonSize":0,"relatedSize":0,"tag":"[今天AAA]成交量"},{"dateText":"前天CCC","indexName":"成交量","indexProperties":["交易日期 前天CCC"],"dateUnit":"日","source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"前天CCC"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_浮点型数值(股)","domain":"abs_股票领域","sonSize":0,"relatedSize":0,"tag":"[今天AAA]成交量"},{"opName":"and","opProperty":"","sonSize":6,"relatedSize":0},{"opName":"/","opProperty":"(=1.8","uiText":"（AAA今天的成交量/5日的区间日均成交量）>=1.8倍","sonSize":2,"queryText":"（AAA今天的成交量/5日的区间日均成交量）>=1.8倍","relatedSize":2},{"dateText":"今天AAA","indexName":"成交量","indexProperties":["交易日期 今天AAA"],"dateUnit":"日","source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"今天AAA"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_浮点型数值(股)","domain":"abs_股票领域","sonSize":0,"relatedSize":0,"tag":"[今天AAA]成交量"},{"dateText":"5日","indexName":"区间日均成交量","indexProperties":["起始交易日期 EEE","截止交易日期 今天AAA"],"dateUnit":"日","source":"new_parser","type":"index","indexPropertiesMap":{"起始交易日期":"EEE","截止交易日期":"今天AAA"},"reportType":"TRADE_DAILY","dateType":"+区间","valueType":"_浮点型数值(股)","domain":"abs_股票领域","sonSize":0,"relatedSize":0,"tag":"[5日]区间日均成交量"},{"opName":"and","opProperty":"","sonSize":2,"relatedSize":0},{"indexName":"股票简称","indexProperties":["不包含st,退"],"source":"new_parser","type":"index","indexPropertiesMap":{"不包含":"st,退"},"reportType":"null","valueType":"_股票简称","domain":"abs_股票领域","uiText":"股票简称不包含st","sonSize":0,"queryText":"股票简称不包含st","relatedSize":0,"tag":"股票简称"},{"indexName":"股票简称","indexProperties":["不包含st,退"],"source":"new_parser","type":"index","indexPropertiesMap":{"不包含":"st,退"},"reportType":"null","valueType":"_股票简称","domain":"abs_股票领域","uiText":"股票简称不包含退","sonSize":0,"queryText":"股票简称不包含退","relatedSize":0,"tag":"股票简称"}]'''
            d1 = datetime.datetime.strptime(str(self.dates[-1]), "%Y-%m-%d").date()
            newDate1 = d1.strftime("%Y%m%d")
            newDate11 = f'''{d1.year}年{d1.month}月{d1.day}日'''
            Condition = Condition.replace("今天AAA",newDate1)
            Condition = Condition.replace("AAA今天",newDate11)
            query = query.replace("今天AAA",newDate1)

            d2 = datetime.datetime.strptime(str(self.dates[-2]), "%Y-%m-%d").date()
            newDate2 = d2.strftime("%Y%m%d")
            newDate22 = f'''{d2.year}年{d2.month}月{d2.day}日'''
            Condition = Condition.replace("昨天BBB",newDate2)
            Condition = Condition.replace("BBB昨天",newDate22)
            query = query.replace("昨天BBB",newDate2)

            d3 = datetime.datetime.strptime(str(self.dates[-3]), "%Y-%m-%d").date()
            newDate3 = d3.strftime("%Y%m%d")
            newDate33 = f'''{d3.year}年{d3.month}月{d3.day}日'''
            Condition = Condition.replace("前天CCC",newDate3)
            Condition = Condition.replace("CCC前天",newDate33)
            query = query.replace("前天CCC",newDate3)

            d5 = datetime.datetime.strptime(str(self.dates[-5]), "%Y-%m-%d").date()
            newDate5 = d5.strftime("%Y%m%d")
            Condition = Condition.replace("EEE",newDate5)

            logger.warning(query)
            # print(Condition)
            ths = CFetchDataFromTHS2(query,Condition)
            ths.page = page
            ths.perPage = perPage
            ths.dateRange0 = newDate5
            ths.dateRange1 = newDate1
            ths.iwc_token = "b1ceb3590f3c93f6d9983031907e7c1c"

            df = ths.RequstData(self.v)
            #print(df)
            map = self.keywordTranslator(df)
            tmpDataFrame = pd.DataFrame()
            for key in map:
                tmpDataFrame[key] = df[map[key]]

            logger.error(f"获取了第 [{page}] 页数据, 每页 [{perPage}] 条, 获取了 [{tmpDataFrame.shape[0]}] 条数据")
            return tmpDataFrame


    def RequestDaliangDataEX(self):
            query = '今天AAA成交量是昨天BBB成交量的1.8倍以上 今天AAA成交量是前天CCC成交量的1.8倍以上 今天AAA成交量是5日平均成交量的1.8倍以上 非st 非退市'
            Condition = '''[{"chunkedResult":"今天AAA成交量是昨天BBB成交量的1.8倍以上 _&_今天AAA成交量是前天CCC成交量的1.8倍以上 _&_今天AAA成交量是5日平均成交量的1.8倍以上 _&_非st _&_非退市","opName":"and","opProperty":"","sonSize":14,"relatedSize":0},{"opName":"/","opProperty":"(=1.8","uiText":"（AAA今天的成交量/BBB昨天的成交量）>=1.8倍","sonSize":2,"queryText":"（AAA今天的成交量/BBB昨天的成交量）>=1.8倍","relatedSize":2},{"dateText":"今天AAA","indexName":"成交量","indexProperties":["交易日期 今天AAA"],"dateUnit":"日","source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"今天AAA"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_浮点型数值(股)","domain":"abs_股票领域","sonSize":0,"relatedSize":0,"tag":"[今天AAA]成交量"},{"dateText":"昨天BBB","indexName":"成交量","indexProperties":["交易日期 昨天BBB"],"dateUnit":"日","source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"昨天BBB"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_浮点型数值(股)","domain":"abs_股票领域","sonSize":0,"relatedSize":0,"tag":"[今天AAA]成交量"},{"opName":"and","opProperty":"","sonSize":10,"relatedSize":0},{"opName":"/","opProperty":"(=1.8","uiText":"（AAA今天的成交量/CCC前天的成交量）>=1.8倍","sonSize":2,"queryText":"（AAA今天的成交量/CCC前天的成交量）>=1.8倍","relatedSize":2},{"dateText":"今天AAA","indexName":"成交量","indexProperties":["交易日期 今天AAA"],"dateUnit":"日","source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"今天AAA"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_浮点型数值(股)","domain":"abs_股票领域","sonSize":0,"relatedSize":0,"tag":"[今天AAA]成交量"},{"dateText":"前天CCC","indexName":"成交量","indexProperties":["交易日期 前天CCC"],"dateUnit":"日","source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"前天CCC"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_浮点型数值(股)","domain":"abs_股票领域","sonSize":0,"relatedSize":0,"tag":"[今天AAA]成交量"},{"opName":"and","opProperty":"","sonSize":6,"relatedSize":0},{"opName":"/","opProperty":"(=1.8","uiText":"（AAA今天的成交量/5日的区间日均成交量）>=1.8倍","sonSize":2,"queryText":"（AAA今天的成交量/5日的区间日均成交量）>=1.8倍","relatedSize":2},{"dateText":"今天AAA","indexName":"成交量","indexProperties":["交易日期 今天AAA"],"dateUnit":"日","source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"今天AAA"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_浮点型数值(股)","domain":"abs_股票领域","sonSize":0,"relatedSize":0,"tag":"[今天AAA]成交量"},{"dateText":"5日","indexName":"区间日均成交量","indexProperties":["起始交易日期 EEE","截止交易日期 今天AAA"],"dateUnit":"日","source":"new_parser","type":"index","indexPropertiesMap":{"起始交易日期":"EEE","截止交易日期":"今天AAA"},"reportType":"TRADE_DAILY","dateType":"+区间","valueType":"_浮点型数值(股)","domain":"abs_股票领域","sonSize":0,"relatedSize":0,"tag":"[5日]区间日均成交量"},{"opName":"and","opProperty":"","sonSize":2,"relatedSize":0},{"indexName":"股票简称","indexProperties":["不包含st,退"],"source":"new_parser","type":"index","indexPropertiesMap":{"不包含":"st,退"},"reportType":"null","valueType":"_股票简称","domain":"abs_股票领域","uiText":"股票简称不包含st","sonSize":0,"queryText":"股票简称不包含st","relatedSize":0,"tag":"股票简称"},{"indexName":"股票简称","indexProperties":["不包含st,退"],"source":"new_parser","type":"index","indexPropertiesMap":{"不包含":"st,退"},"reportType":"null","valueType":"_股票简称","domain":"abs_股票领域","uiText":"股票简称不包含退","sonSize":0,"queryText":"股票简称不包含退","relatedSize":0,"tag":"股票简称"}]'''
            d1 = datetime.datetime.strptime(str(self.dates[-1]), "%Y-%m-%d").date()
            newDate1 = d1.strftime("%Y%m%d")
            newDate11 = f'''{d1.year}年{d1.month}月{d1.day}日'''
            Condition = Condition.replace("今天AAA",newDate1)
            Condition = Condition.replace("AAA今天",newDate11)
            query = query.replace("今天AAA",newDate1)

            d2 = datetime.datetime.strptime(str(self.dates[-2]), "%Y-%m-%d").date()
            newDate2 = d2.strftime("%Y%m%d")
            newDate22 = f'''{d2.year}年{d2.month}月{d2.day}日'''
            Condition = Condition.replace("昨天BBB",newDate2)
            Condition = Condition.replace("BBB昨天",newDate22)
            query = query.replace("昨天BBB",newDate2)

            d3 = datetime.datetime.strptime(str(self.dates[-3]), "%Y-%m-%d").date()
            newDate3 = d3.strftime("%Y%m%d")
            newDate33 = f'''{d3.year}年{d3.month}月{d3.day}日'''
            Condition = Condition.replace("前天CCC",newDate3)
            Condition = Condition.replace("CCC前天",newDate33)
            query = query.replace("前天CCC",newDate3)

            d5 = datetime.datetime.strptime(str(self.dates[-5]), "%Y-%m-%d").date()
            newDate5 = d5.strftime("%Y%m%d")
            Condition = Condition.replace("EEE",newDate5)

            perPage = 100
            ths = CFetchDataFromTHS_MultiPageMgr(query,Condition)
            ths.perPage = perPage
            ths.dateRange0 = newDate5
            ths.dateRange1 = newDate1
            ths.iwc_token = "b1ceb3590f3c93f6d9983031907e7c1c"

            df = ths.RequestMutiPageData(self.v,perPage)
            #print(df)
            map = self.keywordTranslator(df)
            self.dataFrame = pd.DataFrame()
            for key in map:
                self.dataFrame[key] = df[map[key]]

            folder = f'/Volumes/Data/复盘/股票_New/{self.dates[-1]}/'
            if os.path.exists(folder) == False:
                os.makedirs(folder)

            fileName = f'''大量_{self.dates[-1]}'''
            self.DataFrameToJPG(self.dataFrame,["股票代码","股票简称"],folder,fileName)
            return self.dataFrame


    def RequestDailyData(self,page = 1,perPage = 100):
            query = '今天AAA成交量是昨天BBB成交量的1.8倍以上 今天AAA成交量是前天CCC成交量的1.8倍以上 今天AAA成交量是5日平均成交量的1.8倍以上 非st 非退市'
            Condition = '''[{"chunkedResult":"今天AAA成交量是昨天BBB成交量的1.8倍以上 _&_今天AAA成交量是前天CCC成交量的1.8倍以上 _&_今天AAA成交量是5日平均成交量的1.8倍以上 _&_非st _&_非退市","opName":"and","opProperty":"","sonSize":14,"relatedSize":0},{"opName":"/","opProperty":"(=1.8","uiText":"（AAA今天的成交量/BBB昨天的成交量）>=1.8倍","sonSize":2,"queryText":"（AAA今天的成交量/BBB昨天的成交量）>=1.8倍","relatedSize":2},{"dateText":"今天AAA","indexName":"成交量","indexProperties":["交易日期 今天AAA"],"dateUnit":"日","source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"今天AAA"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_浮点型数值(股)","domain":"abs_股票领域","sonSize":0,"relatedSize":0,"tag":"[今天AAA]成交量"},{"dateText":"昨天BBB","indexName":"成交量","indexProperties":["交易日期 昨天BBB"],"dateUnit":"日","source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"昨天BBB"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_浮点型数值(股)","domain":"abs_股票领域","sonSize":0,"relatedSize":0,"tag":"[今天AAA]成交量"},{"opName":"and","opProperty":"","sonSize":10,"relatedSize":0},{"opName":"/","opProperty":"(=1.8","uiText":"（AAA今天的成交量/CCC前天的成交量）>=1.8倍","sonSize":2,"queryText":"（AAA今天的成交量/CCC前天的成交量）>=1.8倍","relatedSize":2},{"dateText":"今天AAA","indexName":"成交量","indexProperties":["交易日期 今天AAA"],"dateUnit":"日","source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"今天AAA"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_浮点型数值(股)","domain":"abs_股票领域","sonSize":0,"relatedSize":0,"tag":"[今天AAA]成交量"},{"dateText":"前天CCC","indexName":"成交量","indexProperties":["交易日期 前天CCC"],"dateUnit":"日","source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"前天CCC"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_浮点型数值(股)","domain":"abs_股票领域","sonSize":0,"relatedSize":0,"tag":"[今天AAA]成交量"},{"opName":"and","opProperty":"","sonSize":6,"relatedSize":0},{"opName":"/","opProperty":"(=1.8","uiText":"（AAA今天的成交量/5日的区间日均成交量）>=1.8倍","sonSize":2,"queryText":"（AAA今天的成交量/5日的区间日均成交量）>=1.8倍","relatedSize":2},{"dateText":"今天AAA","indexName":"成交量","indexProperties":["交易日期 今天AAA"],"dateUnit":"日","source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"今天AAA"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_浮点型数值(股)","domain":"abs_股票领域","sonSize":0,"relatedSize":0,"tag":"[今天AAA]成交量"},{"dateText":"5日","indexName":"区间日均成交量","indexProperties":["起始交易日期 EEE","截止交易日期 今天AAA"],"dateUnit":"日","source":"new_parser","type":"index","indexPropertiesMap":{"起始交易日期":"EEE","截止交易日期":"今天AAA"},"reportType":"TRADE_DAILY","dateType":"+区间","valueType":"_浮点型数值(股)","domain":"abs_股票领域","sonSize":0,"relatedSize":0,"tag":"[5日]区间日均成交量"},{"opName":"and","opProperty":"","sonSize":2,"relatedSize":0},{"indexName":"股票简称","indexProperties":["不包含st,退"],"source":"new_parser","type":"index","indexPropertiesMap":{"不包含":"st,退"},"reportType":"null","valueType":"_股票简称","domain":"abs_股票领域","uiText":"股票简称不包含st","sonSize":0,"queryText":"股票简称不包含st","relatedSize":0,"tag":"股票简称"},{"indexName":"股票简称","indexProperties":["不包含st,退"],"source":"new_parser","type":"index","indexPropertiesMap":{"不包含":"st,退"},"reportType":"null","valueType":"_股票简称","domain":"abs_股票领域","uiText":"股票简称不包含退","sonSize":0,"queryText":"股票简称不包含退","relatedSize":0,"tag":"股票简称"}]'''
            d1 = datetime.datetime.strptime(str(self.dates[-1]), "%Y-%m-%d").date()
            newDate1 = d1.strftime("%Y%m%d")
            newDate11 = f'''{d1.year}年{d1.month}月{d1.day}日'''
            Condition = Condition.replace("今天AAA",newDate1)
            Condition = Condition.replace("AAA今天",newDate11)
            query = query.replace("今天AAA",newDate1)

            d2 = datetime.datetime.strptime(str(self.dates[-2]), "%Y-%m-%d").date()
            newDate2 = d2.strftime("%Y%m%d")
            newDate22 = f'''{d2.year}年{d2.month}月{d2.day}日'''
            Condition = Condition.replace("昨天BBB",newDate2)
            Condition = Condition.replace("BBB昨天",newDate22)
            query = query.replace("昨天BBB",newDate2)

            d3 = datetime.datetime.strptime(str(self.dates[-3]), "%Y-%m-%d").date()
            newDate3 = d3.strftime("%Y%m%d")
            newDate33 = f'''{d3.year}年{d3.month}月{d3.day}日'''
            Condition = Condition.replace("前天CCC",newDate3)
            Condition = Condition.replace("CCC前天",newDate33)
            query = query.replace("前天CCC",newDate3)

            d5 = datetime.datetime.strptime(str(self.dates[-5]), "%Y-%m-%d").date()
            newDate5 = d5.strftime("%Y%m%d")
            Condition = Condition.replace("EEE",newDate5)

            logger.warning(query)
            # print(Condition)
            ths = CFetchDataFromTHS2(query,Condition)
            ths.page = page
            ths.perPage = perPage
            ths.dateRange0 = newDate5
            ths.dateRange1 = newDate1
            ths.iwc_token = "b1ceb3590f3c93f6d9983031907e7c1c"

            df = ths.RequstData(self.v)
            #print(df)
            map = self.keywordTranslator(df)
            self.dataFrame = pd.DataFrame()
            for key in map:
                self.dataFrame[key] = df[map[key]]

            folder = f'/Volumes/Data/复盘/股票_New/{self.dates[-1]}/'
            if os.path.exists(folder) == False:
                os.makedirs(folder)

            fileName = f'''大量_{self.dates[-1]}'''
            self.DataFrameToJPG(self.dataFrame,["股票代码","股票简称"],folder,fileName)
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
                logger.info(fullPath)
                jpgDataFrame = pd.DataFrame(tmp,columns=columns)
                self.ConvertDataFrameToJPG(jpgDataFrame,fullPath)
        else:
            fullPath = f"{rootPath}{fileName}.jpg"
            logger.info(fullPath)
            jpgDataFrame = pd.DataFrame(df,columns=columns)
            self.ConvertDataFrameToJPG(jpgDataFrame,fullPath)