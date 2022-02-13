import pandas as pd
import tushare as ts
from tushareData.tushareToken import TUSHAR_TOKEN
import datetime

TUSHARE_COLUMN_MAP={
    "ts_code":"股票代码",
    "trade_date":"日期",
    "open":"开盘价",
    "high":"最高价",
    "low":"最低价",
    "close":"收盘价",
    "pre_close":"昨收价",
    "pct_chg":"涨跌幅",
    "vol":"成交量",
    "amount":"成交额",
}
class fethTushareDailyData(object):
    def __init__(self):
        ts.set_token(TUSHAR_TOKEN)
        self.api = ts.pro_api()
        
    def FetchDailyData(self,ts_code,start_date,end_date,adj='qfq'):
        df = ts.pro_bar(ts_code=ts_code, adj=adj, start_date=start_date, end_date=end_date)
        returnDF = pd.DataFrame()
        for key in TUSHARE_COLUMN_MAP:
            returnDF[TUSHARE_COLUMN_MAP[key]] = df[key]
        returnDF['成交额'] = returnDF['成交额']*1000
        returnDF['成交量'] = returnDF['成交量']*100
        returnDF['日期'] = returnDF.apply(lambda row: self._str2Timestamp(row['日期']), axis=1)
        pd.set_option('display.float_format',lambda x:'%.2f' % x)
        return returnDF
    
    def FetchDailyDataLastN(self,ts_code,lastN,adj='qfq'):
        today=datetime.date.today() 
        oneday=datetime.timedelta(days=lastN-1) 
        lastN=today-oneday
        return self.FetchDailyData(ts_code,lastN.strftime("%Y%m%d"),today.strftime("%Y%m%d"),adj)
    
    def _str2Timestamp(self,t):
        d = datetime.datetime.strptime(t,'%Y%m%d').date()
        s = d.strftime("%Y-%m-%d")
        return s
    
    def FetchTreadingDate(self,start_date,end_date,):
        df = self.api.trade_cal(start_date=start_date, end_date=end_date)
        returnDF = pd.DataFrame()
        returnDF['交易所'] = df["exchange"]
        returnDF['日期'] = df["cal_date"]
        returnDF['开市'] = df["is_open"]
        returnDF['日期'] = returnDF.apply(lambda row: self._str2Timestamp(row['日期']), axis=1)
        return returnDF