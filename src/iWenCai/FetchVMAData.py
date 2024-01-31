from iWenCai.iWenCaiApi import CIWenCaiAPI
import re
import pandas as pd
import datetime

VMA_DATA_COLUMNS_MAP= {
    '股票代码' : '^股票代码',
    '股票简称' :'^股票简称',
    '涨跌幅'   :"^最新涨跌幅",
    '成交量'   :"^成交量D",
}

class CFetchVMAData(object):
    def __init__(self,dbConnection,today):
        self.dbConnection = dbConnection
        self.today = today
        self.payload = {
                "source": "Ths_iwencai_Xuangu",
                "version": "2.0",
                "query_area": "",
                "block_list": "",
                "add_info": "{\"urp\":{\"scene\":1,\"company\":1,\"business\":1},\"contentType\":\"json\",\"searchInfo\":true}",
                "question": f'''成交量;10日区间日均成交量;20日区间日均成交量;30日区间日均成交量;60日区间日均成交量;90日区间日均成交量;120日区间日均成交量;250日区间日均成交量''',
                "perpage": 100,
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
        if df.empty:
            return None
        
        map = self.keywordTranslator(df)
        self.dataFrame = pd.DataFrame()
        for key in map:
            self.dataFrame[key] = df[map[key]]
        self.dataFrame["日期"] = self.today

        self.dataFrame["涨跌幅"] = self.dataFrame["涨跌幅"].astype(float)
        self.dataFrame["成交量"] = self.dataFrame["成交量"].astype(float)
        self.dataFrame["VMA10"] = self.dataFrame["VMA10"].astype(float)
        self.dataFrame["VMA20"] = self.dataFrame["VMA20"].astype(float)

        self.dataFrame["VMA30"] = self.dataFrame["VMA30"].astype(float)
        self.dataFrame["VMA60"] = self.dataFrame["VMA60"].astype(float)
        self.dataFrame["VMA90"] = self.dataFrame["VMA90"].astype(float)
        self.dataFrame["VMA120"] = self.dataFrame["VMA120"].astype(float)
        self.dataFrame["VMA250"] = self.dataFrame["VMA250"].astype(float)

        self.dataFrame.dropna(inplace=True)


        self.dataFrame['V/MA10'] = self.dataFrame.apply(lambda row: row["成交量"]/row["VMA10"], axis=1)
        self.dataFrame['V/MA20'] = self.dataFrame.apply(lambda row: row["成交量"]/row["VMA20"], axis=1)
        self.dataFrame['V/MA30'] = self.dataFrame.apply(lambda row: row["成交量"]/row["VMA30"], axis=1)
        self.dataFrame['V/MA60'] = self.dataFrame.apply(lambda row: row["成交量"]/row["VMA60"], axis=1)
        self.dataFrame['V/MA90'] = self.dataFrame.apply(lambda row: row["成交量"]/row["VMA90"], axis=1)
        self.dataFrame['V/MA120'] = self.dataFrame.apply(lambda row: row["成交量"]/row["VMA120"], axis=1)
        self.dataFrame['V/MA250'] = self.dataFrame.apply(lambda row: row["成交量"]/row["VMA250"], axis=1)


        self.dataFrame['成交量'] = self.dataFrame.apply(lambda row: self.formatVolumn2(row['成交量']), axis=1)
        self.dataFrame['V/MA10'] = self.dataFrame.apply(lambda row: self.formatVolumn2(row['V/MA10']), axis=1)
        self.dataFrame['V/MA20'] = self.dataFrame.apply(lambda row: self.formatVolumn2(row['V/MA20']), axis=1)
        self.dataFrame['V/MA30'] = self.dataFrame.apply(lambda row: self.formatVolumn2(row['V/MA30']), axis=1)
        self.dataFrame['V/MA60'] = self.dataFrame.apply(lambda row: self.formatVolumn2(row['V/MA60']), axis=1)
        self.dataFrame['V/MA90'] = self.dataFrame.apply(lambda row: self.formatVolumn2(row['V/MA90']), axis=1)
        self.dataFrame['V/MA120'] = self.dataFrame.apply(lambda row: self.formatVolumn2(row['V/MA120']), axis=1)
        self.dataFrame['V/MA250'] = self.dataFrame.apply(lambda row: self.formatVolumn2(row['V/MA250']), axis=1)

        newDf = pd.DataFrame(self.dataFrame, columns=["日期","股票代码","股票简称","涨跌幅","V/MA10","V/MA20","V/MA30","V/MA60","V/MA90","V/MA120","V/MA250"])
        sqls = self._DataFrameToSqls_INSERT_OR_REPLACE(newDf,"stockdaily_vma")
        for sql in sqls:
            self.dbConnection.Execute(sql)
        return self.dataFrame

    def keywordTranslator(self,dataframe):
        columnsKeys = VMA_DATA_COLUMNS_MAP.keys()
        dfKeys = dataframe.columns
        retMap = {}
        for key in columnsKeys:
            value = VMA_DATA_COLUMNS_MAP[key]
            today = f'''\[{self.today.replace("-","")}\]'''
            value = value.replace("D",today)
            for dfKey in dfKeys:
                if re.match(value, dfKey) != None:
                    retMap[key] = dfKey


        for dfKey in dfKeys:
            res = re.search('^区间日均成交量\[(?P<start>\d+)-(?P<end>\d+)\]$',dfKey)
            if res is not None:
                resDict = res.groupdict()
                start = datetime.datetime.strptime(resDict['start'].strip(), '%Y%m%d').date()
                end = datetime.datetime.strptime(resDict['end'].strip(), '%Y%m%d').date()
                date_range = pd.date_range(start, end)
                # 计算工作日数量
                workday_count = len([t for t in date_range if t.day_of_week not in (5,6)])
                if workday_count >=10 and workday_count <20:
                    retMap["VMA10"] = dfKey
                elif workday_count >=20 and workday_count <30:
                    retMap["VMA20"] = dfKey
                elif workday_count >=30 and workday_count <60:
                    retMap["VMA30"] = dfKey
                elif workday_count >=60 and workday_count <90:
                    retMap["VMA60"] = dfKey
                elif workday_count >=90 and workday_count <120:
                    retMap["VMA90"] = dfKey
                elif workday_count >=120 and workday_count <250:
                    retMap["VMA120"] = dfKey
                elif workday_count >=250 :
                    retMap["VMA250"] = dfKey
    
        return retMap
    
    def _DataFrameToSqls_INSERT_OR_REPLACE(self,datas,tableName):
        sqls = []
        for _, row in datas.iterrows():
            index_str = '''`,`'''.join(row.index)
            value_str = '''","'''.join(str(x) for x in row.values)
            sql = '''REPLACE INTO `{0}` (`{1}`) VALUES ("{2}");'''.format(tableName,index_str,value_str)
            sqls.append(sql)
        return sqls