from iWenCai.iWenCaiApi import CIWenCaiAPI
import re
import pandas as pd

STOCK_DAILY_COLUMNS_MAP= {
    '股票代码' : '^股票代码',
    '开盘价'  :"^开盘价:前复权D",
    '收盘价' : '^收盘价:前复权D',
    '最高价' : '^最高价:前复权D',
    '最低价' : '^最低价:前复权D',
    '成交量' : '^成交量D',
    '成交额' : '^成交额D',
    '涨跌幅' :"^涨跌幅:前复权D",
    '上市天数':"^上市天数D",
    '所属概念':"^所属概念$",
}

class CFetchStockDailyData(object):
    def __init__(self,dbConnection,today):
        self.dbConnection = dbConnection
        self.today = today
        self.payload = {
                "source": "Ths_iwencai_Xuangu",
                "version": "2.0",
                "query_area": "",
                "block_list": "",
                "add_info": "{\"urp\":{\"scene\":1,\"company\":1,\"business\":1},\"contentType\":\"json\",\"searchInfo\":true}",
                "question": f'''日期{self.today} 前复权开盘价，前复权收盘价，前复权最高价，前复权最低价，前复权涨跌幅, {self.today}成交量，{self.today}成交额，上市天数,所属概念''',
                "perpage": "100",
                "page": 1,
                "secondary_intent": "stock",
                "log_info": "{\"input_type\":\"typewrite\"}",
                "rsh": "240679370"  
                }
        self.dataFrame = None

    def formatVolumn2(self,volumn):
        ret = f'''{volumn:.2f}'''
        return ret

    def formatVolumn(self,volumn,delta = 1.0):
        newVolumn = float(volumn) * delta
        s = newVolumn /100000000.0 # 除以1亿
        ret = volumn
        if s <1:
            t = newVolumn / 10000.0
            ret = f'''{t:.2f}万'''
        else:
            ret = f'''{s:.2f}亿'''
        return ret
    
    def RequestAllPagesDataAndWriteToDB(self,perPage=100):
        api = CIWenCaiAPI(dbConnection=self.dbConnection)
        df = api.RequestAllPagesData(self.payload,perPage)
        map = self.keywordTranslator(df)
        self.dataFrame = pd.DataFrame()
        for key in map:
            self.dataFrame[key] = df[map[key]]
        self.dataFrame["日期"] = self.today
        # self.dataFrame = self.dataFrame[self.dataFrame['开盘价(点)'].isna().values != True] #删除开盘价没有值的板块
        # self.dataFrame = self.dataFrame[self.dataFrame['收盘价(点)'].isna().values != True] #删除收盘价没有值的板块
        # self.dataFrame = self.dataFrame[self.dataFrame['最高价(点)'].isna().values != True] #删除最高价没有值的板块
        # self.dataFrame = self.dataFrame[self.dataFrame['最低价(点)'].isna().values != True] #删除最低价没有值的板块

        # self.dataFrame["上涨家数(家)"] = self.dataFrame["上涨家数(家)"].fillna(0).astype(int)
        # self.dataFrame["下跌家数(家)"] = self.dataFrame["下跌家数(家)"].fillna(0).astype(int)

        # self.dataFrame["流通市值(元)"] = self.dataFrame["流通市值(元)"].fillna(0).astype(float)
        # self.dataFrame["总市值(元)"] = self.dataFrame["总市值(元)"].fillna(0).astype(float)
        # self.dataFrame["成交量(股)"] = self.dataFrame["成交量(股)"].fillna(0).astype(float)
        # self.dataFrame["成交额(元)"] = self.dataFrame["成交额(元)"].fillna(0).astype(float)

        # self.dataFrame["开盘价(点)"] = self.dataFrame["开盘价(点)"].fillna(0).astype(float)
        # self.dataFrame["收盘价(点)"] = self.dataFrame["收盘价(点)"].fillna(0).astype(float)
        # self.dataFrame["最高价(点)"] = self.dataFrame["最高价(点)"].fillna(0).astype(float)
        # self.dataFrame["最低价(点)"] = self.dataFrame["最低价(点)"].fillna(0).astype(float)
        # self.dataFrame["涨跌幅(%)"] = self.dataFrame["涨跌幅(%)"].fillna(0).astype(float)
        # self.dataFrame["换手率(%)"] = self.dataFrame["换手率(%)"].fillna(0).astype(float)
        # self.dataFrame["量比"] = self.dataFrame["量比"].fillna(0).astype(float)

        

        # self.dataFrame['流通市值(元)'] = self.dataFrame.apply(lambda row: self.formatVolumn(row['流通市值(元)'],1.0), axis=1)
        # self.dataFrame['总市值(元)'] = self.dataFrame.apply(lambda row: self.formatVolumn(row['总市值(元)'],1.0), axis=1)
        # self.dataFrame['成交量(股)'] = self.dataFrame.apply(lambda row: self.formatVolumn(row['成交量(股)'],1.0), axis=1)
        # self.dataFrame['成交额(元)'] = self.dataFrame.apply(lambda row: self.formatVolumn(row['成交额(元)'],1.0), axis=1)

        # self.dataFrame['开盘价(点)'] = self.dataFrame.apply(lambda row: self.formatVolumn2(row['开盘价(点)']), axis=1)
        # self.dataFrame['收盘价(点)'] = self.dataFrame.apply(lambda row: self.formatVolumn2(row['收盘价(点)']), axis=1)
        # self.dataFrame['最高价(点)'] = self.dataFrame.apply(lambda row: self.formatVolumn2(row['最高价(点)']), axis=1)
        # self.dataFrame['最低价(点)'] = self.dataFrame.apply(lambda row: self.formatVolumn2(row['最低价(点)']), axis=1)

        # self.dataFrame['涨跌幅(%)'] = self.dataFrame.apply(lambda row: self.formatVolumn2(row['涨跌幅(%)']), axis=1)
        # self.dataFrame['换手率(%)'] = self.dataFrame.apply(lambda row: self.formatVolumn2(row['换手率(%)']), axis=1)
        # self.dataFrame['量比'] = self.dataFrame.apply(lambda row: self.formatVolumn2(row['量比']), axis=1)
        #print(self.dataFrame)
        sqls = self._DataFrameToSqls_INSERT_OR_REPLACE(self.dataFrame,"stockdailyinfo_test")
        for sql in sqls:
            self.dbConnection.Execute(sql)
        return self.dataFrame

    def keywordTranslator(self,dataframe):
        columnsKeys = STOCK_DAILY_COLUMNS_MAP.keys()
        dfKeys = dataframe.columns
        retMap = {}
        for key in columnsKeys:
            value = STOCK_DAILY_COLUMNS_MAP[key]
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