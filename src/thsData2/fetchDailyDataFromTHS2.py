import pandas as pd
import re
from thsData2.fetchDataFromTHS2 import CFetchDataFromTHS2
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
    '上市日期': '^新股上市日期',
    '所属概念': '^所属概念$',
    "所属概念数量":"^所属概念数量",
    '行业':'^所属同花顺行业',
    '流通市值':"^a股市值",
    '上市天数':"^上市天数",
}
logger = logging.getLogger()
class CFetchDailyDataFromTHS2(object):
    def __init__(self,date,v):
        self.dataFrame = None
        self.date = date
        self.v = v
        self.dataSize = 0

    def RequestDailyData(self,page = 1,perPage = 100):
        query = '前复权开盘价，前复权收盘价，前复权最高价，前复权最低价，前复权涨跌幅, 成交量，成交额，上市天数,所属概念'
        Condition = '''[{"chunkedResult":"前复权开盘价,_&_前复权收盘价,_&_前复权最高价,_&_前复权最低价,_&_前复权涨跌幅, _&_成交量,_&_成交额,_&_上市天数,_&_所属概念","opName":"and","opProperty":"","sonSize":16,"relatedSize":0},{"indexName":"开盘价:前复权","indexProperties":["nodate 1","交易日期 20220902"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20220902","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_浮点型数值(元)","domain":"abs_股票领域","uiText":"开盘价:前复权","sonSize":0,"queryText":"开盘价:前复权","relatedSize":0,"tag":"开盘价:前复权"},{"opName":"and","opProperty":"","sonSize":14,"relatedSize":0},{"indexName":"收盘价:前复权","indexProperties":["nodate 1","交易日期 20220902"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20220902","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_浮点型数值(元)","domain":"abs_股票领域","uiText":"收盘价:前复权","sonSize":0,"queryText":"收盘价:前复权","relatedSize":0,"tag":"收盘价:前复权"},{"opName":"and","opProperty":"","sonSize":12,"relatedSize":0},{"indexName":"最高价:前复权","indexProperties":["nodate 1","交易日期 20220902"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20220902","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_浮点型数值(元)","domain":"abs_股票领域","uiText":"最高价:前复权","sonSize":0,"queryText":"最高价:前复权","relatedSize":0,"tag":"最高价:前复权"},{"opName":"and","opProperty":"","sonSize":10,"relatedSize":0},{"indexName":"最低价:前复权","indexProperties":["nodate 1","交易日期 20220902"],"source":"new_parser","type":"tech","indexPropertiesMap":{"交易日期":"20220902","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_浮点型数值(元)","domain":"abs_股票领域","uiText":"最低价:前复权","sonSize":0,"queryText":"最低价:前复权","relatedSize":0,"tag":"最低价:前复权"},{"opName":"and","opProperty":"","sonSize":8,"relatedSize":0},{"indexName":"涨跌幅:前复权","indexProperties":["nodate 1","复权方式 前复权","交易日期 20220902"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20220902","复权方式":"前复权","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_浮点型数值(%)","domain":"abs_股票领域","uiText":"前复权的涨跌幅","sonSize":0,"queryText":"前复权的涨跌幅","relatedSize":0,"tag":"涨跌幅:前复权"},{"opName":"and","opProperty":"","sonSize":6,"relatedSize":0},{"indexName":"成交量","indexProperties":["nodate 1","交易日期 20220902"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20220902","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_浮点型数值(股)","domain":"abs_股票领域","uiText":"成交量","sonSize":0,"queryText":"成交量","relatedSize":0,"tag":"成交量"},{"opName":"and","opProperty":"","sonSize":4,"relatedSize":0},{"indexName":"成交额","indexProperties":["nodate 1","交易日期 20220902"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20220902","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_浮点型数值(元|港元|美元|英镑)","domain":"abs_股票领域","uiText":"成交额","sonSize":0,"queryText":"成交额","relatedSize":0,"tag":"成交额"},{"opName":"and","opProperty":"","sonSize":2,"relatedSize":0},{"indexName":"上市天数","indexProperties":["nodate 1","交易日期 20220902"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20220902","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_整型数值(天)","domain":"abs_股票领域","uiText":"上市天数","sonSize":0,"queryText":"上市天数","relatedSize":0,"tag":"上市天数"},{"indexName":"所属概念","indexProperties":[],"source":"new_parser","type":"index","indexPropertiesMap":{},"reportType":"null","valueType":"_所属概念","domain":"abs_股票领域","uiText":"所属概念","sonSize":0,"queryText":"所属概念","relatedSize":0,"tag":"所属概念"}]'''
        d = datetime.datetime.strptime(str(self.date), "%Y-%m-%d").date()
        newDate = d.strftime("%Y%m%d")

        Condition = Condition.replace("20220902",newDate)
        ths = CFetchDataFromTHS2(query,Condition)
        ths.page = page
        ths.perPage = perPage
        ths.dateRange0 = newDate
        ths.dateRange1 = newDate
        ths.iwc_token = "0ac952af16654771178418035"

        logger.warning(query)
        df = ths.RequstData(self.v)
        self.dataSize = df.shape[0]  #记录总共获取了多少条数据
        map = self.keywordTranslator(df)
        self.dataFrame = pd.DataFrame()
        for key in map:
            self.dataFrame[key] = df[map[key]]
        
        self.dataFrame = self.dataFrame[self.dataFrame['上市天数'].isna().values != True] #删除未上市的
        self.dataFrame['上市天数'] = self.dataFrame['上市天数'].apply(lambda x:int(x)).astype(int)
        self.dataFrame['成交量'] = self.dataFrame['成交量'].astype(float).astype(str)
        self.dataFrame['成交额'] = self.dataFrame['成交额'].astype(float).astype(str)
        self.dataFrame['涨跌幅'] = self.dataFrame['涨跌幅'].astype(float).apply(lambda x:'''%.3f'''%x)
        logger.info(str(self.dataFrame))
        logger.info(str(self.dataFrame.shape))


    def keywordTranslator(self,dataframe):
        columnsKeys = DAILY_COLUMNS_MAP.keys()
        dfKeys = dataframe.columns
        retMap = {}
        for key in columnsKeys:
            value = DAILY_COLUMNS_MAP[key]
            for dfKey in dfKeys:
                if re.match(value, dfKey) != None:
                    retMap[key] = dfKey
                       
        return retMap

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


    def _DataFrameToSqls_INSERT_OR_REPLACE(self,datas,tableName):
        sqls = []
        for _, row in datas.iterrows():
            index_str = '''`,`'''.join(row.index)
            value_str = '''","'''.join(str(x) for x in row.values)
            sql = '''REPLACE INTO `{0}` (`{1}`) VALUES ("{2}");'''.format(tableName,index_str,value_str)
            sqls.append(sql)
        return sqls




def GetDailyDataMgr(date,v,stockBasicInfo,stockDailyInfo):
    step = 100
    sql_Basic = []
    sql_Daily = []
    threshold = 4500
    totalSize = 0
    for pageID in range(1,5000):
        logger.error(f"ready to fetch page:{pageID}")
        dailyFetcher = CFetchDailyDataFromTHS2(date,v)
        dailyFetcher.RequestDailyData(pageID,step)
        basicSqls = dailyFetcher.FormateBacicInfoToSQL(stockBasicInfo)
        sql_Basic.extend(basicSqls)
            
        dailySqls = dailyFetcher.FormateDailyInfoToSQL(stockDailyInfo)
        sql_Daily.extend(dailySqls)

        totalSize  = totalSize + dailyFetcher.dataSize
        #起码要获取4500个信息以上
        if totalSize>threshold and (dailyFetcher.dataSize != step):
            break

    
    return (sql_Basic,sql_Daily)
