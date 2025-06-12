import datetime
import schedule
import time
from ColoredLog import StartToInitLogger
from mysql.connect2DB import ConnectToDB
import sys
from stockSelect.pattern1 import CStockPattern1
from stockSelect.pattern2 import CStockPattern2
from stockSelect.pattern3 import CStockPattern3
from stockSelect.pattern4 import CStockPattern4
from stockSelect.pattern5 import CStockPattern5  # N形战法
from stockSelect.pattern6 import CStockPattern6  # 吸筹跌破反包战法
from stockSelect.pattern8 import CStockPattern8  # 吸筹跌破反包战法
from stockSelect.pattern9 import CStockPattern9  # 吸筹跌破反包战法
from stockSelect.pattern10 import CStockPattern10  # 倍量
from stockSelect.pattern12 import CStockPattern12  # 51020上穿战法
from stockSelect.N_Pattern import CNPattern,CNPatternEx
from stockSelect.pattern13 import CPattern13


def StockSelectMain():
    dbConnection = ConnectToDB()
    p = CNPattern(dbConnection)
    p.Select()

    

if __name__ == "__main__":
    dbConnection = ConnectToDB()
    # p = CStockPattern1(dbConnection)
    # p.Select()
    # p = CStockPattern2(dbConnection)
    # p.Select()
    # p = CStockPattern5(dbConnection)
    # p.Select()

    # p = CStockPattern10(dbConnection)
    # p.SelectLast()
    # p = CStockPattern6(dbConnection)
    # p.Select()

    # p = CStockPattern9(dbConnection)
    # p.SelectAll()

    # p = CStockPattern12(dbConnection)
    # p.SelectLast()
    # p.FillPercentages()
    # p.UpdatedateShenglvPeilv()
    # p.SelectLastDayData()

    p = CNPattern(dbConnection)
    p.Select()
    
    p = CNPatternEx(dbConnection)
    p.Select()


    # p = CPattern13(dbConnection)
    # p.Select()