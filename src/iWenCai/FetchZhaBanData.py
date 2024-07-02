from iWenCai.iWenCaiApi import CIWenCaiAPI
import re
import pandas as pd

BANKUAI_INDEX_COLUMNS_MAP= {
    '股票代码' : '^股票代码',
    '股票名称' :'^股票简称',
    '涨跌幅' : '^最新涨跌幅',
    '首次涨停时间' : '^首次涨停时间D',
    '涨停开板次数' : '^涨停开板次数D',
    '涨停板封板时长' : '^涨停封板时长D',
    '涨停价' : '^涨停价',
}

class CFetchZhaBanDailyData(object):
    def __init__(self,dbConnection,today):
        self.dbConnection = dbConnection
        self.today = today
        self.payload = {
                "source": "Ths_iwencai_Xuangu",
                "version": "2.0",
                "query_area": "",
                "block_list": "",
                "add_info": "{\"urp\":{\"scene\":1,\"company\":1,\"business\":1},\"contentType\":\"json\",\"searchInfo\":true}",
                "question": f'''{self.today.replace("-",".")} 曾经涨停 非st 非退市 非北交所''',
                "perpage": "100",
                "page": 1,
                "secondary_intent": "stock",
                "log_info": "{\"input_type\":\"typewrite\"}",
                "rsh": "240679370"  
                }
        
        self.dataFrame = None

    def formatVolumn(self,volumn):
        ret = f'''{volumn:.2f}'''
        return ret

    def RequestAllPagesDataAndWriteToDB(self,perPage=50):
        api = CIWenCaiAPI(dbConnection=self.dbConnection)
        df = api.RequestAllPagesData(self.payload,perPage)
        if df.empty:
            return None
        
        map = self.keywordTranslator(df)
        self.dataFrame = pd.DataFrame()
        for key in map:
            self.dataFrame[key] = df[map[key]]
        self.dataFrame["日期"] = self.today

        self.dataFrame["涨跌幅"] = self.dataFrame["涨跌幅"].fillna(0).astype(float)
        self.dataFrame["涨停开板次数"] = self.dataFrame["涨停开板次数"].fillna(0).astype(int)
        self.dataFrame['涨停板封板时长'] = self.dataFrame.apply(lambda row: self.formatVolumn(row['涨停板封板时长']), axis=1)
        self.dataFrame['涨跌幅'] = self.dataFrame.apply(lambda row: self.formatVolumn(row['涨跌幅']), axis=1)

        sqls = self._DataFrameToSqls_INSERT_OR_REPLACE(self.dataFrame,"stockdaily_zhaban")
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