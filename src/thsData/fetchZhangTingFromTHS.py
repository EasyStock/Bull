
from thsData.fetchDataFromTHS import CFetchDataFromTHS
import re
import pandas as pd
import time
import datetime
import logging

logger = logging.getLogger()

ZHANGTING_COLUMNS_MAP= {
    '股票代码' : '^股票代码',
    '股票简称' :'^股票简称',
    '上市天数':"^上市天数",
    '连续涨停天数':"^连续涨停天数",
    '涨停原因类别' : '^涨停原因类别',
    '首次涨停时间' : '^首次涨停时间',
    '最终涨停时间' : '^最终涨停时间',
}

class CZhangTingResult(object):
    def __init__(self,zhangTingReason):
        self.zhangTingReason = zhangTingReason
        self.stockIDs = []
        self.stockNames = []
        
    def Insert(self,stockID,stockName):
        if stockID not in self.stockIDs:
            self.stockIDs.append(stockID)

        if stockName not in self.stockNames:
            self.stockNames.append(stockName)
        
    def Count(self):
        return len(self.stockIDs)
    
    def __str__(self):
        count = self.Count()
        msg = 'Count:   %s\n'%(count)
        for stockID in self.stockIDs:
            msg = msg + '++++++' + stockID + '\n'
        
        return msg
    
    def FormatToDict(self):
        return {
            '涨停原因':self.zhangTingReason,
            '数量':self.Count(),
            '股票代码s':str(self.stockIDs),
            '股票名称s':str(self.stockNames),
        }
        
class CFetchZhangTingDataFromTHS(object):
    def __init__(self,cookie,v):
        self.dataFrame = None
        self.date = None
        self.keyWords = ' 非st,非新股,连续涨停天数大于0,涨停原因类别,首次涨停时间,最终涨停时间'
        self.url = 'http://x.iwencai.com/stockpick/search?typed=1&preParams=&ts=1&f=1&qs=result_rewrite&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=%C2%A0%E9%9D%9Est%2C%E9%9D%9E%E6%96%B0%E8%82%A1%2C%E8%BF%9E%E7%BB%AD%E6%B6%A8%E5%81%9C%E5%A4%A9%E6%95%B0%E5%A4%A7%E4%BA%8E0%2C%E6%B6%A8%E5%81%9C%E5%8E%9F%E5%9B%A0%E7%B1%BB%E5%88%AB%2C%E9%A6%96%E6%AC%A1%E6%B6%A8%E5%81%9C%E6%97%B6%E9%97%B4%2C%E6%9C%80%E7%BB%88%E6%B6%A8%E5%81%9C%E6%97%B6%E9%97%B4&queryarea='
        self.referer = 'http://x.iwencai.com/stockpick/search?typed=1&preParams=&ts=1&f=1&qs=result_rewrite&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=%C2%A0%E9%9D%9Est%2C%E9%9D%9E%E6%96%B0%E8%82%A1%2C%E8%BF%9E%E7%BB%AD%E6%B6%A8%E5%81%9C%E5%A4%A9%E6%95%B0%E5%A4%A7%E4%BA%8E0%2C%E6%B6%A8%E5%81%9C%E5%8E%9F%E5%9B%A0%E7%B1%BB%E5%88%AB%2C%E9%A6%96%E6%AC%A1%E6%B6%A8%E5%81%9C%E6%97%B6%E9%97%B4%2C%E6%9C%80%E7%BB%88%E6%B6%A8%E5%81%9C%E6%97%B6%E9%97%B4&queryarea='
        self.cookie = cookie
        self.v = v
        self.zhangTingDF = None
        self.Reasons = {}
        
    def GetZhangTingData(self):
        fetcher = CFetchDataFromTHS(self.cookie,self.url, self.referer, self.v)
        result = fetcher.FetchAllInOne()
        map = self.keywordTranslator(result)
        self.dataFrame = pd.DataFrame()
        for key in map:
            self.dataFrame[key] = result[map[key]]
        
        self.dataFrame = self.dataFrame[self.dataFrame['上市天数'] != '--'] #删除未上市的
        self.dataFrame['首次涨停时间'] = self.dataFrame.apply(lambda row: self._timestampToStr(row['首次涨停时间']), axis=1)
        self.dataFrame['最终涨停时间'] = self.dataFrame.apply(lambda row: self._timestampToStr(row['最终涨停时间']), axis=1)
        logger.info(f'{self.dataFrame[:10]}\n{self.dataFrame[-10:]}')
        logger.info(self.date)
        logger.info(f'{self.dataFrame.columns}')
        logger.info(f'{self.dataFrame.shape}')
        
    def _timestampToStr(self,t):
        d = datetime.datetime.fromtimestamp(float(t)/1000)
        s = d.strftime("%H:%M:%S")
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
        columns = ['涨停原因','数量',"股票代码s",'股票名称s']
        result = []
        if self.dataFrame is None:
            return result

        DailyInfoDF = pd.DataFrame(self.dataFrame,columns = columns)
        DailyInfoDF['日期']=self.date
        result = self._DataFrameToSqls_INSERT_OR_REPLACE(DailyInfoDF,tableName)
        return result
    
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

    def _DataFrameToSqls_INSERT_OR_REPLACE(self,datas,tableName):
        sqls = []
        for _, row in datas.iterrows():
            index_str = '''`,`'''.join(row.index)
            value_str = '''","'''.join(str(x) for x in row.values)
            sql = '''REPLACE INTO `{0}` (`{1}`) VALUES ("{2}");'''.format(tableName,index_str,value_str)
            sqls.append(sql)
        return sqls