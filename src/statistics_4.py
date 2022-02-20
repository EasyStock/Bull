from DBOperating import GetTradingDateLastN,GetZhangTingDataBy,GetZhangTingData,GetRemainZhangTingDataBy
from mysql.connect2DB import ConnectToDB
from categrate import CATEGRAGTE

class CZhangTing(object):
    def __init__(self):
        self.zhangTingDays = 0
        self.zhangTingReason = None
        self.gaiNian = None
        self.firstZhangTingTime = None
        self.lastZhangTingTime = None
        self.stockName = None
        self.stockID = None
        self.gaiNianList = []
        self.zhangTingReasonList = []
        self.date = None
    
    def isBigThan(self,other):
        if self.zhangTingDays > other.zhangTingDays:
            return True
        elif self.zhangTingDays == other.zhangTingDays:
            if(self.firstZhangTingTime < other.firstZhangTingTime):
                return True
            elif (self.firstZhangTingTime == other.firstZhangTingTime):
                return (self.lastZhangTingTime < other.lastZhangTingTime)
            else:
                return False
        else:
            return False
    
    def InitWithSeries(self,row):
        self.date = row["日期"]
        self.stockID = row["股票代码"]
        self.stockName = row["股票简称"]
        self.zhangTingDays = row["连续涨停天数"]
        self.zhangTingReason = row["涨停原因类别"]
        self.firstZhangTingTime = row["首次涨停时间"]
        self.lastZhangTingTime = row["最终涨停时间"]
        self.gaiNian = row["所属概念"]
        self._parserGaiNian()
        self._parserZhangTingReason()
        
    def _parserGaiNian(self):
        if self.gaiNian is None:
            return
        
        self.gaiNianList = self.gaiNian.split(';')
            
    def _parserZhangTingReason(self):
        if self.zhangTingReason is None:
            return
        self.zhangTingReasonList = self.zhangTingReason.split('+')
        
    
    def __str__(self) -> str:
        msg = f'''
        {"股票代码:":<16}   {self.stockID:<}
        {"股票简称:":<16}   {self.stockName}
        {"连续涨停天数:":<16}   {self.zhangTingDays}
        {"涨停原因类别:":<16}   {self.zhangTingReason}
        {"首次涨停时间:":<16}   {self.firstZhangTingTime}
        {"最终涨停时间:":<16}   {self.lastZhangTingTime}
        {"所属概念:":<16}   {self.gaiNian}
        '''
        return msg

def AnalysisZhangTingReason(dbConnection):
    result = GetTradingDateLastN(dbConnection,15)
    date = result[-1]
    df = GetZhangTingData(dbConnection,date)
    print(df)
    reasons = []
    for index, row in df.iterrows():
        reason = row["涨停原因类别"]
        reasons.extend(reason.split("+"))

    reasons = list(set(reasons))
    reasonResults = {}
    for reason in reasons:
        sql = f"select count(*) from `stockZhangting`AS A, `stockBasicInfo` As B where A.`股票代码`= B.`股票代码` and `日期` = '{date}' and A.`涨停原因类别` like '%{reason}%' ;"
        result ,_ = dbConnection.Query(sql)
        count = result[0][0]
        reasonResults[reason] = count
    
    ret = sorted(reasonResults.items(), key=lambda d: d[1],reverse=True)
    for r in ret:
        print(r)

def categrateZhangTing(dbConnection):
    import pandas as pd
    pd.set_option('display.max_rows', None)
    result = GetTradingDateLastN(dbConnection,15)
    date = result[-1]
        
    remain = []
    remain1 = []
    for key in CATEGRAGTE:
        print(f"\n=========={key}=============")
        df = GetZhangTingDataBy(dbConnection,date,CATEGRAGTE[key][0],CATEGRAGTE[key][1])
        remain.extend(CATEGRAGTE[key][0])
        remain1.extend(CATEGRAGTE[key][1])
        print(df)
    
    print(f"\n==========剩余=============")
    df = GetRemainZhangTingDataBy(dbConnection,date,remain,remain1)
    print(df)
    
if __name__ == "__main__":
    dbConnection = ConnectToDB()
    AnalysisZhangTingReason(dbConnection)
    categrateZhangTing(dbConnection)