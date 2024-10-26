from iWenCai.iWenCaiApi import CIWenCaiAPI
import re
import pandas as pd

YEWU_COLUMNS_MAP= {
    '股票代码' : '^股票代码',
    '股票简称' :'^股票简称',
    '产品' : '^主营产品名称',
    '范围' : '^经营范围',
    '行业' : '^所属同花顺行业',
}

class CFetchYeWuData(object):
    def __init__(self,dbConnection,today):
        self.dbConnection = dbConnection
        self.today = today
        self.payload = {
                "source": "Ths_iwencai_Xuangu",
                "version": "2.0",
                "query_area": "",
                "block_list": "",
                "add_info": "{\"urp\":{\"scene\":1,\"company\":1,\"business\":1},\"contentType\":\"json\",\"searchInfo\":true}",
                "question": f'''主营业务''',
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
        self.dataFrame["产品"] = self.dataFrame["产品"].str.replace("||","  ")
        self.dataFrame["范围"] = self.dataFrame["范围"]
        self.dataFrame["行业"] = self.dataFrame["行业"]
        sqls = self._DataFrameToSqls_INSERT_OR_REPLACE(self.dataFrame,"stockyewu")
        step = 1
        groupedSql = [" ".join(sqls[i:i+step]) for i in range(0,len(sqls),step)]
        for sql in groupedSql:
            self.dbConnection.Execute(sql)
        return self.dataFrame

    def keywordTranslator(self,dataframe):
        columnsKeys = YEWU_COLUMNS_MAP.keys()
        dfKeys = dataframe.columns
        retMap = {}
        for key in columnsKeys:
            value = YEWU_COLUMNS_MAP[key]
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
            value_str = '''","'''.join(str(x).replace("'","\\'").replace('"','\\"') for x in row.values)
            sql = '''REPLACE INTO `{0}` (`{1}`) VALUES ("{2}");'''.format(tableName,index_str,value_str)
            sqls.append(sql)
        return sqls