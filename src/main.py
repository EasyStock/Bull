#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   main.py
@Time    :   2020/10/29 22:37:13
@Author  :   JianPing Huang 
@Contact :   yuchonghuang@126.com
'''

from ColoredLog import StartToInitLogger
from mysql.connect2DB import ConnectToDB
from tushareMain import UpdateTradingDate,FetchAllInOne
import schedule
import time


# To Do list
# 20CM 划分
# 10CM/20CM 首板奖励率
# 10CM/20CM 连板奖励率
# 关键个股的封单量
# 昨日3板以上今日没有连板同学的list


if __name__ == "__main__":
    pass