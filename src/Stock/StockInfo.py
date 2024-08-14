from Stock.KLine import CKLine
import pandas as pd
import re

THRESHOLD_MAP= [
    ("^000001.SH",2.5),     #上证指数
    ("^000016.SH",2.5),     #深证成指
    ("^000300.SH",2.5),     #沪深300
    ("^000688.SH",2.5),     #科创50
    ("^000852.SH",2.5),     #中证1000
    ("^000905.SH",2.5),     #中证500
    ("^399001.SZ",2.5),     #深证成指
    ("^399006.SZ",2.5),     #创业板指

    ("^00.*",5),
    ("^30.*",8),
    ("^60.*",5),
    ("^68.*",8),
    ("^8.*|^920.*",8),
]


class CStockInfo:
    pass


class CStockInfo(object):
    def __init__(self) -> None:
        self.stockID = ""
        self.stockName = ""
        self.date = ""
        self.kline = CKLine()
        self.volumn = 0.0
        self.yestoday_close = None
        self.zhangfu = None
    
    def initWithSeries(self, series: pd.Series):
        self.stockID = series['stockID']
        self.stockName = series['stockName']
        self.date = series['date']
        self.kline.initWithSeries(series)
        self.volumn = round(series['volumn'],2)
        if "yestoday_close" in series:
            self.yestoday_close = round(series['yestoday_close'],2)
            self.zhangfu = round(series['zhangfu'],2)
    
    @staticmethod
    def create_from_series(series: pd.Series) -> CStockInfo:
        info = CStockInfo()
        info.initWithSeries(series)
        return info
    
    def __str__(self) -> str:
        msg = f'''
        股票代码:               {self.stockID},
        股票名称:               {self.stockName},
        日期:                   {self.date},
        成交量:                 {self.volumn},
        昨日收盘价:             {self.yestoday_close},
        涨幅:                   {self.zhangfu}%,
        开盘价:                 {self.kline.open},
        收盘价:                 {self.kline.close},
        最高价:                 {self.kline.high},
        最低价:                 {self.kline.low},
        振幅:                   {self.kline.total},
        K线实体:                {self.kline.body},    {(0 if self.kline.total == 0 else self.kline.body/self.kline.total*100):.1f}%,
        下影线:                 {self.kline.shadowDown},    {(0 if self.kline.total == 0 else self.kline.shadowDown/self.kline.total*100):.1f}%,
        上影线:                 {self.kline.shadowUp},    {(0 if self.kline.total ==0 else self.kline.shadowUp/self.kline.total*100):.1f}%,
        是否是大阳线:           {self.isBigRedLine(5)}
        是否是pinBar:           {self.isPinBar()}'''
        return msg

    def isPinBar(self) -> bool:
        if (self.zhangfu is None) or pd.isna(self.zhangfu):
            return self.kline.isPinBar()
        else:
            fudu = self.kline.total/self.yestoday_close*100
            for item in THRESHOLD_MAP:
                if re.match(item[0], self.stockID) != None:
                    if fudu >= item[1] and self.kline.isPinBar():
                        return True
                    return False
            
            return False
            
    
        
    def isBigRedLine(self, threshold=5) -> bool:
        if self.zhangfu is None:
            return self.kline.isBigRedLine(threshold)
        else:
            return self.kline.isBigRedLine(threshold) and self.zhangfu >= threshold


    def isYiZhi(self):
        return self.kline.isYiZhi()