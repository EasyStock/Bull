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

if __name__ == "__main__":
    dbConnection = ConnectToDB()
    # p = CStockPattern1(dbConnection)
    # p.Select()
    # p = CStockPattern2(dbConnection)
    # p.Select()
    # p = CStockPattern3(dbConnection,12)
    # p.Select()

    p = CStockPattern5(dbConnection)
    p.Select()
    # p = CStockPattern6(dbConnection)
    # p.Select()

    # p = CStockPattern9(dbConnection)
    # p.SelectAll()