from unittest import result
import pandas as pd
import re
from thsData2.fetchDataFromTHS2 import CFetchDataFromTHS2,CFetchDataFromTHS_MultiPageMgr
import datetime
import logging

ZHANGTING_COLUMNS_MAP= {
    '股票代码' : '^股票代码',
    '股票简称' :'^股票简称',
    '上市天数':"^上市天数",
    '连续涨停天数':"^连续涨停天数",
    '涨停原因类别' : '^涨停原因类别',
    '首次涨停时间' : '^首次涨停时间',
    '最终涨停时间' : '^最终涨停时间',
}

logger = logging.getLogger()
class CFetchZhangTingDataFromTHS2(object):
    def __init__(self,date,v):
        self.dataFrame = None
        self.date = date
        self.v = v

    # def RequestZhangTingData(self):
    #     step = 100
    #     for pageID in range(1,500):
    #         tmpData = self._RequsetZhangTingData(pageID,step)
    #         if self.dataFrame is None:
    #             self.dataFrame = tmpData
    #         else:
    #             self.dataFrame = pd.concat([self.dataFrame,tmpData],ignore_index=True)

    #         size = tmpData.shape[0]
    #         if size < step:
    #             break
        
    #     if self.dataFrame.empty:
    #         return

    #     logger.warning(f"total fetch {pageID} page(s),totalSize = {self.dataFrame.shape[0]}")
    #     self.dataFrame = self.dataFrame[self.dataFrame['上市天数'] != '--'] #删除未上市的
    #     self.dataFrame['首次涨停时间'] = self.dataFrame.apply(lambda row: self._timestampToStr(row['首次涨停时间']), axis=1)
    #     self.dataFrame['最终涨停时间'] = self.dataFrame.apply(lambda row: self._timestampToStr(row['最终涨停时间']), axis=1)

    # def _RequsetZhangTingData(self,page = 1,perPage = 100):
    #         query = '非st,非新股,连续涨停天数大于0,涨停原因类别,首次涨停时间,最终涨停时间'
    #         Condition = '''[{"chunkedResult":"非st,_&_非新股,_&_连续涨停天数大于0,_&_涨停原因类别,_&_首次涨停时间,_&_最终涨停时间","opName":"and","opProperty":"","sonSize":10,"relatedSize":0},{"indexName":"股票简称","indexProperties":["不包含st"],"source":"new_parser","type":"index","indexPropertiesMap":{"不包含":"st"},"reportType":"null","valueType":"_股票简称","domain":"abs_股票领域","uiText":"股票简称不包含st","sonSize":0,"queryText":"股票简称不包含st","relatedSize":0,"tag":"股票简称"},{"opName":"and","opProperty":"","sonSize":8,"relatedSize":0},{"indexName":"上市天数","indexProperties":["nodate 1","交易日期 20220909","(20"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20220909","(":"20","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_整型数值(天)","domain":"abs_股票领域","uiText":"上市天数>20","sonSize":0,"queryText":"上市天数>20","relatedSize":0,"tag":"上市天数"},{"opName":"and","opProperty":"","sonSize":6,"relatedSize":0},{"indexName":"连续涨停天数","indexProperties":["nodate 1","交易日期 20220909","(0"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20220909","(":"0","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_整型数值(天)","domain":"abs_股票领域","uiText":"连续涨停天数>0日","sonSize":0,"queryText":"连续涨停天数>0日","relatedSize":0,"tag":"连续涨停天数"},{"opName":"and","opProperty":"","sonSize":4,"relatedSize":0},{"indexName":"涨停原因类别","indexProperties":["nodate 1","交易日期 20220909"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20220909","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_涨停原因类别","domain":"abs_股票领域","uiText":"涨停原因类别","sonSize":0,"queryText":"涨停原因类别","relatedSize":0,"tag":"涨停原因类别"},{"opName":"and","opProperty":"","sonSize":2,"relatedSize":0},{"indexName":"首次涨停时间","indexProperties":["nodate 1","交易日期 20220909"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20220909","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_整型数值","domain":"abs_股票领域","uiText":"首次涨停时间","sonSize":0,"queryText":"首次涨停时间","relatedSize":0,"tag":"首次涨停时间"},{"indexName":"最终涨停时间","indexProperties":["nodate 1","交易日期 20220909"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20220909","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_整型数值","domain":"abs_股票领域","uiText":"最终涨停时间","sonSize":0,"queryText":"最终涨停时间","relatedSize":0,"tag":"最终涨停时间"}]'''
    #         d = datetime.datetime.strptime(str(self.date), "%Y-%m-%d").date()
    #         newDate = d.strftime("%Y%m%d")

    #         Condition = Condition.replace("20220909",newDate)
    #         ths = CFetchDataFromTHS2(query,Condition)
    #         ths.page = page
    #         ths.perPage = perPage
    #         ths.dateRange0 = newDate
    #         ths.dateRange1 = newDate
    #         ths.iwc_token = "0ac952b216652356192695879"

    #         logger.warning(query)
    #         # print(Condition)
    #         df = ths.RequstData(self.v)
    #         map = self.keywordTranslator(df)
    #         tmpDataFrame = pd.DataFrame()
    #         for key in map:
    #             tmpDataFrame[key] = df[map[key]]
            
    #         logger.error(f"fetch page = {page}, perPage = {perPage}, total = {tmpDataFrame.shape[0]}")
    #         return tmpDataFrame


    def RequestZhangTingDataEX(self):
            query = '非st,非新股,连续涨停天数大于0,涨停原因类别,首次涨停时间,最终涨停时间'
            Condition = '''[{"chunkedResult":"非st,_&_非新股,_&_连续涨停天数大于0,_&_涨停原因类别,_&_首次涨停时间,_&_最终涨停时间","opName":"and","opProperty":"","sonSize":10,"relatedSize":0},{"indexName":"股票简称","indexProperties":["不包含st"],"source":"new_parser","type":"index","indexPropertiesMap":{"不包含":"st"},"reportType":"null","valueType":"_股票简称","domain":"abs_股票领域","uiText":"股票简称不包含st","sonSize":0,"queryText":"股票简称不包含st","relatedSize":0,"tag":"股票简称"},{"opName":"and","opProperty":"","sonSize":8,"relatedSize":0},{"indexName":"上市天数","indexProperties":["nodate 1","交易日期 20220909","(20"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20220909","(":"20","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_整型数值(天)","domain":"abs_股票领域","uiText":"上市天数>20","sonSize":0,"queryText":"上市天数>20","relatedSize":0,"tag":"上市天数"},{"opName":"and","opProperty":"","sonSize":6,"relatedSize":0},{"indexName":"连续涨停天数","indexProperties":["nodate 1","交易日期 20220909","(0"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20220909","(":"0","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_整型数值(天)","domain":"abs_股票领域","uiText":"连续涨停天数>0日","sonSize":0,"queryText":"连续涨停天数>0日","relatedSize":0,"tag":"连续涨停天数"},{"opName":"and","opProperty":"","sonSize":4,"relatedSize":0},{"indexName":"涨停原因类别","indexProperties":["nodate 1","交易日期 20220909"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20220909","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_涨停原因类别","domain":"abs_股票领域","uiText":"涨停原因类别","sonSize":0,"queryText":"涨停原因类别","relatedSize":0,"tag":"涨停原因类别"},{"opName":"and","opProperty":"","sonSize":2,"relatedSize":0},{"indexName":"首次涨停时间","indexProperties":["nodate 1","交易日期 20220909"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20220909","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_整型数值","domain":"abs_股票领域","uiText":"首次涨停时间","sonSize":0,"queryText":"首次涨停时间","relatedSize":0,"tag":"首次涨停时间"},{"indexName":"最终涨停时间","indexProperties":["nodate 1","交易日期 20220909"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20220909","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_整型数值","domain":"abs_股票领域","uiText":"最终涨停时间","sonSize":0,"queryText":"最终涨停时间","relatedSize":0,"tag":"最终涨停时间"}]'''
            d = datetime.datetime.strptime(str(self.date), "%Y-%m-%d").date()
            newDate = d.strftime("%Y%m%d")

            perPage = 100
            Condition = Condition.replace("20220909",newDate)
            ths = CFetchDataFromTHS_MultiPageMgr(query,Condition)
            ths.page = perPage
            ths.perPage = perPage
            ths.dateRange0 = newDate
            ths.dateRange1 = newDate
            ths.iwc_token = "0ac952b216652356192695879"

            df = ths.RequestMutiPageData(self.v,perPage)
            map = self.keywordTranslator(df)
            self.dataFrame = pd.DataFrame()
            for key in map:
                self.dataFrame[key] = df[map[key]]
            
            self.dataFrame = self.dataFrame[self.dataFrame['上市天数'] != '--'] #删除未上市的
            self.dataFrame['首次涨停时间'] = self.dataFrame.apply(lambda row: self._timestampToStr(row['首次涨停时间']), axis=1)
            self.dataFrame['最终涨停时间'] = self.dataFrame.apply(lambda row: self._timestampToStr(row['最终涨停时间']), axis=1)



    def _timestampToStr(self,t):
        # d = datetime.datetime.fromtimestamp(float(t)/1000)
        # s = d.strftime("%H:%M:%S")
        s = t.strip()
        return s
    
    def FormateZhangTingInfoToSQL(self,tableName):
        columns = ['股票代码','股票简称',"连续涨停天数",'涨停原因类别','首次涨停时间','最终涨停时间']
        result = []
        if self.dataFrame is None:
            return result

        self.zhangTingDF = pd.DataFrame(self.dataFrame,columns = columns)
        self.zhangTingDF['日期'] = self.date
        result = self._DataFrameToSqls_INSERT_OR_REPLACE(self.zhangTingDF,tableName)
        return result
    
    def FormateZhangTingReasonInfoToSQL(self,tableName):
        columns = ['涨停原因','数量',"股票代码",'股票名称']
        result = []
        if self.dataFrame is None:
            return result

        DailyInfoDF = pd.DataFrame(self.dataFrame,columns = columns)
        DailyInfoDF['日期']=self.date
        result = self._DataFrameToSqls_INSERT_OR_REPLACE(DailyInfoDF,tableName)
        return result
    
        
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

    def _DataFrameToSqls_INSERT_OR_REPLACE(self,datas,tableName):
        sqls = []
        for _, row in datas.iterrows():
            index_str = '''`,`'''.join(row.index)
            value_str = '''","'''.join(str(x) for x in row.values)
            sql = '''REPLACE INTO `{0}` (`{1}`) VALUES ("{2}");'''.format(tableName,index_str,value_str)
            sqls.append(sql)
        return sqls

