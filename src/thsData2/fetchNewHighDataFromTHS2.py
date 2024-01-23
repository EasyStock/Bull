import pandas as pd
import re
from thsData2.fetchDataFromTHS2 import CFetchDataFromTHS_MultiPageMgr,CFetchDataFromTHS2
import datetime
import logging
import os
from workspace import workSpaceRoot,WorkSpaceFont

NEWHIGH_COLUMNS_MAP= {
    '股票代码' : '^股票代码',
    '股票简称' :'^股票简称',
    '上市天数':"^上市天数",
    '所属概念':"^所属概念$",
    '所属概念数量':"^所属概念数量",
}

logger = logging.getLogger()
class CFetchNewHighDataFromTHS2(object):
    def __init__(self,date,v):
        self.dataFrame = None
        self.date = date
        self.v = v

    def RequestNewHighDataEX(self):
            query = '创历史新高 上市天数大于200 非st 非退市,所属概念'
            Condition = '''[{"chunkedResult":"创历史新高 _&_上市天数大于200 _&_非st _&_非退市,_&_所属概念","opName":"and","opProperty":"","sonSize":8,"relatedSize":0},{"indexName":"股价创历史新高","indexProperties":["nodate 1","交易日期 20220909"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20220909","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_是否","domain":"abs_股票领域","uiText":"股价创历史新高","sonSize":0,"queryText":"股价创历史新高","relatedSize":0,"tag":"股价创历史新高"},{"opName":"and","opProperty":"","sonSize":6,"relatedSize":0},{"indexName":"上市天数","indexProperties":["nodate 1","交易日期 20220909","(200"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20220909","(":"200","nodate":"1"},"reportType":"TRADE_DAILY","dateType":"交易日期","valueType":"_整型数值(天)","domain":"abs_股票领域","uiText":"上市天数>200天","sonSize":0,"queryText":"上市天数>200天","relatedSize":0,"tag":"上市天数"},{"opName":"and","opProperty":"","sonSize":4,"relatedSize":0},{"indexName":"股票简称","indexProperties":["不包含st,退"],"source":"new_parser","type":"index","indexPropertiesMap":{"不包含":"st,退"},"reportType":"null","valueType":"_股票简称","domain":"abs_股票领域","uiText":"股票简称不包含st","sonSize":0,"queryText":"股票简称不包含st","relatedSize":0,"tag":"股票简称"},{"opName":"and","opProperty":"","sonSize":2,"relatedSize":0},{"indexName":"股票简称","indexProperties":["不包含st,退"],"source":"new_parser","type":"index","indexPropertiesMap":{"不包含":"st,退"},"reportType":"null","valueType":"_股票简称","domain":"abs_股票领域","uiText":"股票简称不包含退","sonSize":0,"queryText":"股票简称不包含退","relatedSize":0,"tag":"股票简称"},{"indexName":"所属概念","indexProperties":[],"source":"new_parser","type":"index","indexPropertiesMap":{},"reportType":"null","valueType":"_所属概念","domain":"abs_股票领域","uiText":"所属概念","sonSize":0,"queryText":"所属概念","relatedSize":0,"tag":"所属概念"}]'''
            d = datetime.datetime.strptime(str(self.date), "%Y-%m-%d").date()
            newDate = d.strftime("%Y%m%d")
            perPage = 100
            Condition = Condition.replace("20220909",newDate)
            ths = CFetchDataFromTHS_MultiPageMgr(query,Condition)
            ths.perPage = perPage
            ths.dateRange0 = newDate
            ths.dateRange1 = newDate
            ths.iwc_token = "0ac9666116652356504797468"
            logger.warning(query)
            #print(Condition)
            df = ths.RequestMutiPageData(self.v,perPage)
            #print(df)
            if df is None :
                return
            map = self.keywordTranslator(df)
            self.dataFrame = pd.DataFrame()
            for key in map:
                self.dataFrame[key] = df[map[key]]
            if self.dataFrame.empty:
                return

            self.dataFrame = self.dataFrame[self.dataFrame['上市天数'].isna().values != True] #删除未上市的
            self.dataFrame['上市天数'] = self.dataFrame['上市天数'].apply(lambda x:int(x)).astype(int)


            self.ParserGaiNian()

            rootFolder = f"{workSpaceRoot}/复盘/股票/{self.date}/"
            if os.path.exists(rootFolder) == False:
                os.makedirs(rootFolder)

            fullPath = f"{rootFolder}新高_{self.date}.jpg"
            jpgDataFrame = pd.DataFrame(self.dataFrame,columns=["股票代码","股票简称"])
            self.ConvertDataFrameToJPG(jpgDataFrame,fullPath)
            logger.info(fullPath)

    def ConvertDataFrameToJPG(self, df,fullPath):
        if df.empty:
            return
    
        from pandas.plotting import table
        import matplotlib.pyplot as plt
        plt.rcParams["font.sans-serif"] = [WorkSpaceFont]#显示中文字体
        high = int(0.174 * df.shape[0]+0.5)+1
        fig = plt.figure(figsize=(3, high), dpi=200)#dpi表示清晰度
        ax = fig.add_subplot(111, frame_on=False) 
        ax.xaxis.set_visible(False)  # hide the x axis
        ax.yaxis.set_visible(False)  # hide the y axis
        table(ax, df, loc='center')  # 将df换成需要保存的dataframe即可
        plt.savefig(fullPath)
        plt.close()
    
    def keywordTranslator(self,dataframe):
        columnsKeys = NEWHIGH_COLUMNS_MAP.keys()
        dfKeys = dataframe.columns
        retMap = {}
        for key in columnsKeys:
            value = NEWHIGH_COLUMNS_MAP[key]
            for dfKey in dfKeys:
                if re.match(value, dfKey) != None:
                    retMap[key] = dfKey
                
        return retMap


    def ParserGaiNian(self):
        if self.dataFrame is None:
            return
        
        gaiNian = {

        }
        exceptions = ["转融券标的","融资融券","深股通","沪股通","标普道琼斯A股","MSCI概念","富时罗素概念","富时罗素概念股","B转H",]
        for _,row in self.dataFrame.iterrows():
            stockName = row["股票简称"]
            stockID = row["股票代码"]
            fName = f"{stockID}_{stockName}"
            if row["所属概念数量"] == 0:
                continue
            gaiNians = row["所属概念"].split(";")
            for g in gaiNians:
                if g in exceptions:
                    continue
                if g not in gaiNian:
                    gaiNian[g] = []
                
                gaiNian[g].append(fName)
        
        #print(json.dumps(gaiNian,sort_keys=True,indent=2,separators=(",",": "),ensure_ascii=False))
        t = sorted(gaiNian.items(),key=lambda x:len(x[1]),reverse=True)
        for k in t:
            s = ""
            step = 10
            count = int(len(k[1])/step)
            for index in range(count+1):
                start = index*step
                end = index*step + step
                if end > len(k[1]):
                    end = len(k[1])

                subInfo = k[1][start:end]
                s = s + "   ".join(subInfo) + "\n       "

            res = f'''{k[0]:15} {len(k[1]):5}
**************************************
       {s}'''
            if len((k[1])) >=2:
                logging.error(res)
