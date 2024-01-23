from iWenCai.iWenCaiApi import CIWenCaiAPI
import re
import pandas as pd

BANKUAI_INDEX_COLUMNS_MAP= {
    '转债代码' : '^可转债@可转债代码',
    '转债名称' :'^可转债@可转债简称',
    '正股代码':"^可转债@正股代码",
    '最高价' : '^可转债@最高价D',
    '最低价' : '^可转债@最低价D',
    '开盘价' : '^可转债@开盘价D',
    '收盘价' : '^可转债@收盘价D',
    '成交量' : '^可转债@成交量D',
    '成交额':  "^可转债@成交额D",
    '涨跌幅' : '^可转债@涨跌幅D',
    '上市日期' : '^可转债@上市日期',
}

class CFetchKeZhuanZaiDailyData(object):
    def __init__(self,dbConnection,today):
        self.dbConnection = dbConnection
        self.today = today
        self.payload = {"source":"Ths_iwencai_Xuangu",
                        "version":"2.0",
                        "query_area":"",
                        "block_list":"",
                        "add_info":"{\"urp\":{\"scene\":1,\"company\":1,\"business\":1},\"contentType\":\"json\",\"searchInfo\":true}",
                        "question":f'''{self.today.replace("-",".")} 可转债 最高价 最低价 开盘价 收盘价 成交量 成交额  涨跌幅 上市日期''',
                        "perpage":"100",
                        "page":1,
                        "secondary_intent":"conbond",
                        "log_info":"{\"input_type\":\"typewrite\"}",
                        "rsh":"240679370"
                        }

        self.dataFrame = None

    def formatVolumn(self,volumn):
        ret = f'''{volumn:.2f}'''
        return ret

    def RequestAllPagesDataAndWriteToDB(self,perPage=100):
        api = CIWenCaiAPI(dbConnection=self.dbConnection)
        df = api.RequestAllPagesData(self.payload,perPage)
        if df.empty:
            return None
        
        map = self.keywordTranslator(df)
        self.dataFrame = pd.DataFrame()
        for key in map:
            self.dataFrame[key] = df[map[key]]
        self.dataFrame["日期"] = self.today
        self.dataFrame = self.dataFrame[self.dataFrame['开盘价'].isna().values != True] #删除开盘价没有值的板块
        self.dataFrame = self.dataFrame[self.dataFrame['收盘价'].isna().values != True] #删除收盘价没有值的板块
        self.dataFrame = self.dataFrame[self.dataFrame['最高价'].isna().values != True] #删除最高价没有值的板块
        self.dataFrame = self.dataFrame[self.dataFrame['最低价'].isna().values != True] #删除最低价没有值的板块

        self.dataFrame['转债代码'] =  self.dataFrame['转债代码'].str.replace("\..*","",regex=True)

        self.dataFrame["成交量"] = self.dataFrame["成交量"].fillna(0).astype(float)
        self.dataFrame["成交额"] = self.dataFrame["成交额"].fillna(0).astype(float)
        self.dataFrame["开盘价"] = self.dataFrame["开盘价"].fillna(0).astype(float)
        self.dataFrame["收盘价"] = self.dataFrame["收盘价"].fillna(0).astype(float)
        self.dataFrame["最高价"] = self.dataFrame["最高价"].fillna(0).astype(float)
        self.dataFrame["最低价"] = self.dataFrame["最低价"].fillna(0).astype(float)
        self.dataFrame["涨跌幅"] = self.dataFrame["涨跌幅"].fillna(0).astype(float)

        self.dataFrame['开盘价'] = self.dataFrame.apply(lambda row: self.formatVolumn(row['开盘价']), axis=1)
        self.dataFrame['收盘价'] = self.dataFrame.apply(lambda row: self.formatVolumn(row['收盘价']), axis=1)
        self.dataFrame['最高价'] = self.dataFrame.apply(lambda row: self.formatVolumn(row['最高价']), axis=1)
        self.dataFrame['最低价'] = self.dataFrame.apply(lambda row: self.formatVolumn(row['最低价']), axis=1)
        self.dataFrame['涨跌幅'] = self.dataFrame.apply(lambda row: self.formatVolumn(row['涨跌幅']), axis=1)

        sqls = self._DataFrameToSqls_INSERT_OR_REPLACE(self.dataFrame,"kezhuanzai_ths")
        for sql in sqls:
            self.dbConnection.Execute(sql)
        return self.dataFrame

    def keywordTranslator(self,dataframe):
        columnsKeys = BANKUAI_INDEX_COLUMNS_MAP.keys()
        dfKeys = dataframe.columns
        retMap = {}
        for key in columnsKeys:
            value = BANKUAI_INDEX_COLUMNS_MAP[key]
            today = f'''\[{self.today.replace("-","")}\]'''
            value = value.replace("D",today)
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