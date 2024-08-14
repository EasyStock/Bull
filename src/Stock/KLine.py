import pandas as pd

class CKLine:
    pass


class CKLine(object):
    def __init__(self):
        self.open = 0.0
        self.close = 0.0
        self.high = 0.0
        self.low = 0.0
        self.body = 0.0
        self.shadowDown = 0.0
        self.shadowUp = 0.0
    
    @staticmethod
    def create_from_series(series: pd.Series) -> CKLine:
        kline = CKLine()
        kline.initWithSeries(series)
        return kline
    
    @staticmethod
    def merge_klines(series_list: list[pd.Series]) -> CKLine:
        if len(series_list) <= 1:
            raise ValueError("Series list must contain at least two series")
        
        df = pd.DataFrame(series_list)
        first = CKLine.create_from_series(series_list[0])
        last = CKLine(series_list[-1])
        kline = CKLine()

        kline.open = first.open
        kline.close = last.close
        kline.low = df['low'].min()
        kline.high = df['high'].max()
        return kline

    def initWithSeries(self, series: pd.Series):
        self.open = series['open']
        self.close = series['close']
        self.high = series['high']
        self.low = series['low']
        self.body = round(abs(self.close - self.open),2)
        self.total = round(self.high - self.low,2)
        self.shadowDown = round(abs(min(self.open,self.close) - self.low),2)
        self.shadowUp = round(abs(self.high - max(self.open,self.close)),2)

    def initWithTuple(self, open, close, high, low):
        self.open = open
        self.close = close
        self.high = high
        self.low = low
        self.body = round(abs(self.close - self.open),2)
        self.total = round(self.high - self.low,2)
        self.shadowDown = round(abs(min(self.open,self.close) - self.low),2)
        self.shadowUp = round(abs(self.high - max(self.open,self.close)),2)

    def isRedLine(self):
        return self.close > self.open
    
    def isGreenLine(self):
        return self.close < self.open
    
    def isBlackLine(self):
        return self.close == self.open
    
    def isPinBar(self,threshold_short=0.1,threshold_long=0.667):
        if self.shadowDown > self.shadowUp:
            return (self.shadowDown > self.total * threshold_long and self.shadowUp <= self.total * threshold_short)
        else:
            return (self.shadowUp > self.total * threshold_long and self.shadowDown <= self.total * threshold_short)
    
    def __str__(self) -> str:

        msg = f'''
        开盘价:                 {self.open},
        收盘价:                 {self.close},
        最高价:                 {self.high},
        最低价:                 {self.low},
        振幅:                   {self.total},
        K线实体:                {self.body},    {self.body/self.total*100:.1f}%,
        上影线:                 {self.shadowDown},    {(0 if self.total ==0 else self.shadowDown/self.total*100):.1f}%,
        下影线:                 {self.shadowUp},    {(0 if self.total ==0 else self.shadowUp/self.total*100):.1f}%,
        是否是大阳线:           {self.isBigRedLine(5)}
        是否是pinBar:           {self.isPinBar()}'''
        return msg

    def isBigRedLine(self, threshold=5):
        if (self.total / self.low)*100 < threshold: #振幅要大于threshold
            return False
        
        if self.body < self.total * 0.8:  # 实体要占80%
            return False
        
        return True
    
    def isYiZhi(self):
        return self.open == self.close == self.high == self.low