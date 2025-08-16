
import pandas as pd
import datetime
import pytz
from workspace import GetStockFolder

class CBanKuaiFinishedRatio(object):
    def __init__(self,dbConnection):
        self.dbConnection = dbConnection
        self.lastNDays = 100

    def GetTradingDates(self):
        today = datetime.datetime.now(pytz.timezone('Asia/Shanghai')).date()
        end = today.strftime("%Y-%m-%d")
        sql = f"SELECT `日期` FROM stock.treadingDay where `开市` =1 and `交易所`='SSE' and `日期`<='{end}' order by `日期` DESC limit {self.lastNDays};"
        res,_ = self.dbConnection.Query(sql)
        self.tradingDays = [r[0] for r in reversed(res)]
        return self.tradingDays
    

    def GetBanKuaiData(self,date):
        sql1 = f'''
        SELECT B.`日期`,A.`股票代码`,A.`股票简称` As `股票名称` ,A.`行业`,B.`开盘价`,B.`收盘价` ,B.`最低价`,B.`最高价`,B.`成交量` FROM stock.stockyewu AS A,(
            SELECT `日期`,`股票代码`,`开盘价`,`收盘价`,`最低价`,`最高价`,`成交量` FROM stock.stockdailyinfo_2022 where `日期` = "{date}" 
            UNION 
            SELECT `日期`,`股票代码`,`开盘价`,`收盘价`,`最低价`,`最高价`,`成交量` FROM stock.stockdailyinfo_2023 where `日期` = "{date}" 
            UNION 
            SELECT `日期`,`股票代码`,`开盘价`,`收盘价` ,`最低价`,`最高价`,`成交量` FROM stock.stockdailyinfo where `日期` = "{date}"
            ) AS B where A.`股票代码` = B.`股票代码` 
        '''

        result1,columns1 = self.dbConnection.Query(sql1)
        newDf1=pd.DataFrame(result1,columns=columns1)
        newDf1["开盘价"] = newDf1["开盘价"].astype(float)
        newDf1["收盘价"] = newDf1["收盘价"].astype(float)
        newDf1["最低价"] = newDf1["最低价"].astype(float)
        newDf1["最高价"] = newDf1["最高价"].astype(float)
        newDf1["成交量"] = newDf1["成交量"].astype(float)
        newDf1[['一级行业', '二级行业', '三级行业']] = newDf1['行业'].str.split('-', expand=True, n=2)
        return newDf1
    

    def _ProcessFininshedRatio(self,hangye,group,startDate,endDate,startDataFrame,endDataFrame):
            size = group.shape[0]
            result = {}
            result["行业"] = hangye
            result["总数"] = size
            result["开始时间"] = startDate
            result["结束时间"] = endDate
            result["高于"] = []
            result["低于"] = []
            for index,row in group.iterrows():
                stockID = row["股票代码"]
                close = row["收盘价"]

                df2 = startDataFrame[startDataFrame["股票代码"] == stockID]
                if df2.empty == True:
                    continue

                row2= df2.iloc[0]
                close2 = row2["收盘价"]
                if close >= close2:
                    result["高于"].append(stockID)
                else:
                    result["低于"].append(stockID)

            result["高于完成率"] = float(f'''{len(result["高于"])/size*100:.2f}''')
            result["低于完成率"] = float(f'''{len(result["低于"])/size*100:.2f}''')

            return result


    def _GroupAndProcess(self,startDate,endDate,startDataFrame,endDataFrame,name = "行业"):
        groups = endDataFrame.groupby(name)
        res_hangYe = []
        for hangye, group in groups:
            result = self._ProcessFininshedRatio(hangye,group,startDate,endDate,startDataFrame,endDataFrame)
            res_hangYe.append(result)


        resultDF = pd.DataFrame(data=res_hangYe,columns=["行业","总数","高于完成率","低于完成率","开始时间","结束时间","高于","低于"])
        resultDF.sort_values(['高于完成率',"总数"],axis=0,ascending=False,inplace=True)
        resultDF.reset_index(drop=True,inplace=True)
        root = GetStockFolder(endDate)
        resultDF.to_excel(f'''{root}{startDate}-{endDate}-{name}完成率.xlsx''')

    def FininshedRatio(self,startDate,endDate = None):
        if endDate is None:
            endDate =self.GetTradingDates()[-1]

        
        startDataFrame = self.GetBanKuaiData(startDate)
        endDataFrame = self.GetBanKuaiData(endDate)

        #self._GroupAndProcess(startDate,endDate,startDataFrame,endDataFrame,"行业")
        self._GroupAndProcess(startDate,endDate,startDataFrame,endDataFrame,"一级行业")
        self._GroupAndProcess(startDate,endDate,startDataFrame,endDataFrame,"二级行业")
        self._GroupAndProcess(startDate,endDate,startDataFrame,endDataFrame,"三级行业")



        # groups = endDataFrame.groupby("行业")
        # res_hangYe = []
        # for hangye, group in groups:
        #     result = self._ProcessFininshedRatio(hangye,group,startDate,endDate,startDataFrame,endDataFrame)
        #     res_hangYe.append(result)


        # groups = endDataFrame.groupby("一级行业")
        # res_hangYe_1 = []
        # for hangye, group in groups:
        #     result = self._ProcessFininshedRatio(hangye,group,startDate,endDate,startDataFrame,endDataFrame)
        #     res_hangYe_1.append(result)

        # groups = endDataFrame.groupby("二级行业")
        # res_hangYe_2 = []
        # for hangye, group in groups:
        #     result = self._ProcessFininshedRatio(hangye,group,startDate,endDate,startDataFrame,endDataFrame)
        #     res_hangYe_2.append(result)

        # groups = endDataFrame.groupby("三级行业")
        # res_hangYe_3 = []
        # for hangye, group in groups:
        #     result = self._ProcessFininshedRatio(hangye,group,startDate,endDate,startDataFrame,endDataFrame)
        #     res_hangYe_3.append(result)


        
        # resultDF = pd.DataFrame(data=res,columns=["行业","总数","高于完成率","低于完成率","开始时间","结束时间","高于","低于"])
        # resultDF.sort_values(['高于完成率',"总数"],axis=0,ascending=False,inplace=True)
        # resultDF.reset_index(drop=True,inplace=True)
        # resultDF.to_excel("/tmp/行业完成率.xlsx")
        # print(resultDF)




