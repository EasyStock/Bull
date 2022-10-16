import os
from mysql.connect2DB import ConnectToDB
from fupan.tradingDate import GetTradingDateLastN

import logging
logger = logging.getLogger()



def FuPanSummary1(dbConnection,tradingDay):
    sql = f'''SELECT `日期`,`股票代码`,`股票简称`,`涨停原因类别`,`封单915`,`封单925` FROM stock.yiziban where `日期` = "{tradingDay}"  order by `连续涨停天数` DESC;'''
    
