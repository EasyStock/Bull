
import pandas as pd
import re
from thsData.fetchDataFromTHS import CFetchDataFromTHS
import datetime
import logging

DAILY_COLUMNS_MAP= {
    '股票代码' : '^股票代码',
    '股票简称' :'^股票简称',
    '涨跌幅':"^涨跌幅:前复权",
    '开盘价' : '^开盘价:前复权',
    '收盘价' : '^收盘价:前复权',
    '最高价' : '^最高价:前复权',
    '最低价' : '^最低价:前复权',
    '成交量': '^成交量',
    '成交额': '^成交额',
    '上市日期': '^上市日期',
    '所属概念': '^所属概念$',
    "所属概念数量":"^所属概念数量",
    '行业':'^所属同花顺行业',
    '流通市值':"^a股流通市值",
    '上市天数':"^上市天数",
}
logger = logging.getLogger()
class CFetchDailyDataFromTHS(object):
    def __init__(self,cookie,v):
        self.dataFrame = None
        self.date = None
        self.keyWords = '前复权开盘价，前复权收盘价，前复权最高价，前复权最低价，前复权涨跌幅, 成交量，成交额，上市天数,所属概念'
        self.url = 'http://x.iwencai.com/stockpick/search?ts=1&f=1&qs=stockhome_topbar_click&w=%E5%89%8D%E5%A4%8D%E6%9D%83%E5%BC%80%E7%9B%98%E4%BB%B7%EF%BC%8C%E5%89%8D%E5%A4%8D%E6%9D%83%E6%94%B6%E7%9B%98%E4%BB%B7%EF%BC%8C%E5%89%8D%E5%A4%8D%E6%9D%83%E6%9C%80%E9%AB%98%E4%BB%B7%EF%BC%8C%E5%89%8D%E5%A4%8D%E6%9D%83%E6%9C%80%E4%BD%8E%E4%BB%B7%EF%BC%8C%E5%89%8D%E5%A4%8D%E6%9D%83%E6%B6%A8%E8%B7%8C%E5%B9%85,%20%E6%88%90%E4%BA%A4%E9%87%8F%EF%BC%8C%E6%88%90%E4%BA%A4%E9%A2%9D%EF%BC%8C%E4%B8%8A%E5%B8%82%E5%A4%A9%E6%95%B0,%E6%89%80%E5%B1%9E%E6%A6%82%E5%BF%B5'
        self.referer = 'http://x.iwencai.com/stockpick/search?typed=1&preParams=&ts=1&f=1&qs=result_rewrite&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=%E5%89%8D%E5%A4%8D%E6%9D%83%E5%BC%80%E7%9B%98%E4%BB%B7%EF%BC%8C%E5%89%8D%E5%A4%8D%E6%9D%83%E6%94%B6%E7%9B%98%E4%BB%B7%EF%BC%8C%E5%89%8D%E5%A4%8D%E6%9D%83%E6%9C%80%E9%AB%98%E4%BB%B7%EF%BC%8C%E5%89%8D%E5%A4%8D%E6%9D%83%E6%9C%80%E4%BD%8E%E4%BB%B7%EF%BC%8C%E5%89%8D%E5%A4%8D%E6%9D%83%E6%B6%A8%E8%B7%8C%E5%B9%85%2C%20%E6%88%90%E4%BA%A4%E9%87%8F%EF%BC%8C%E6%88%90%E4%BA%A4%E9%A2%9D%EF%BC%8C%E4%B8%8A%E5%B8%82%E6%97%A5%E6%9C%9F%2C%E6%89%80%E5%B1%9E%E6%A6%82%E5%BF%B5&queryarea='
        self.cookie = cookie
        self.v = v
        
    def GetDailyData(self):
        fetcher = CFetchDataFromTHS(self.cookie,self.url, self.referer, self.v)
        result = fetcher.FetchAllInOne()
        map = self.keywordTranslator(result)
        self.dataFrame = pd.DataFrame()
        for key in map:
            self.dataFrame[key] = result[map[key]]
        
        print(self.dataFrame)
        self.dataFrame = self.dataFrame[self.dataFrame['上市天数'] != '--'] #删除未上市的
        
        logger.info(str(self.dataFrame[:10]))
        logger.info(str(self.dataFrame[-10:]))

        logger.info(str(self.dataFrame.columns))
        logger.info(str(self.dataFrame.shape))
    
    def FormateBacicInfoToSQL(self,tableName):
        columns = ['股票代码','股票简称','所属概念',"所属概念数量",'上市日期','上市天数','行业','流通市值']
        result = []
        if self.dataFrame is None:
            return result
        
        today = datetime.date.today()
        basicInfoDF = pd.DataFrame(self.dataFrame,columns = columns)
        basicInfoDF['更新日期'] = str(today)
        result = self._DataFrameToSqls_INSERT_OR_REPLACE(basicInfoDF,tableName)
        return result
    
    def FormateDailyInfoToSQL(self,tableName):
        columns = ['股票代码','开盘价',"收盘价",'最高价','最低价','成交量','成交额','涨跌幅']
        result = []
        if self.dataFrame is None:
            return result

        DailyInfoDF = pd.DataFrame(self.dataFrame,columns = columns)
        DailyInfoDF['日期']=self.date
        result = self._DataFrameToSqls_INSERT_OR_REPLACE(DailyInfoDF,tableName)
        return result
    
    def _parserDate(self,key):
        k = '开盘价:前复权(元)<br>'
        if key.find(k) != -1:
            year,month,day = key[key.find(k)+len(k):].split('.')
            self.date = '%s-%s-%s'%(year,month,day)
        
    def keywordTranslator(self,dataframe):
        columnsKeys = DAILY_COLUMNS_MAP.keys()
        dfKeys = dataframe.columns
        retMap = {}
        for key in columnsKeys:
            value = DAILY_COLUMNS_MAP[key]
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
    
    
