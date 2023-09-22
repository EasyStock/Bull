from logging import log
from thsData.fetchDailyDataFromTHS import CFetchDailyDataFromTHS
from thsData.constants_10jqka import eng_10jqka_CookieList
import random
from mysql.mysql import CMySqlConnection
from thsData.fetchZhangTingFromTHS import CFetchZhangTingDataFromTHS
from thsData.fetchNewHighDataFromTHS import CNewHighDataFromTHS
from mysql.connect2DB import ConnectToDB
from ColoredLog import StartToInitLogger
import schedule
import time
import datetime
import base64
from urllib.parse import quote
from thsData.fetchZhangTingLanBanFromTHS import CFetchZhangTingLanBanFromTHS
from thsData.fetchDaliangDataFromTHS import CFetchDaLiangFromTHS
from thsData.fetchDaliang_LanBanDataFromTHS import CFetchDaLiangAndLanBanFromTHS
from thsData.fetchNoZhangTingLanBanData import CFetchNoZhangTingData
from fupan.tradingDate import GetTradingDateLastN


ZHANGTING_TABLE_NAME = 'zhangting'
ZHANGTING_REASON_TABLE_NAME = 'zhangtingreason'

cookie = "ta_random_userid=5yqsx6jxak; WafStatus=0; cid=6d07b3804ba608a64d44b53b9531a7a91650871235; ComputerID=6d07b3804ba608a64d44b53b9531a7a91650871235; PHPSESSID=0a1c33ad4bdcb6901ca58a383612f311; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjU4MzY0MzgzOjo6MTQzMDEzNjkwMDo0MDA0MTc6MDoxMjRiZmQ1ZTc5ODBkNTU4MGIxN2JlNmYwMjlhMmQ1MjE6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=320dd44c4bb53749369a65274368f53a; user_status=0; utk=eeb0549bcf3de92c942f9b1183adb72b;"
cookie ="ta_random_userid=5yqsx6jxak; WafStatus=0; cid=6d07b3804ba608a64d44b53b9531a7a91650871235; ComputerID=6d07b3804ba608a64d44b53b9531a7a91650871235; PHPSESSID=0a1c33ad4bdcb6901ca58a383612f311; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjU5NTI0ODU3Ojo6MTQzMDEzNjkwMDo0MDI3NDM6MDoxZGRlNmYwNmY1M2FiZDlkMzI5NzM4ZjdiODYzNTJjNjY6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=240b5cb9cd8fc15fcf52bd3137d16c64; user_status=0; utk=0f3ed8c1f3707af24e84e247d5c81d41;"
cookie = "ta_random_userid=5yqsx6jxak; WafStatus=0; cid=6d07b3804ba608a64d44b53b9531a7a91650871235; ComputerID=6d07b3804ba608a64d44b53b9531a7a91650871235; PHPSESSID=0a1c33ad4bdcb6901ca58a383612f311; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjU5OTU3OTY2Ojo6MTQzMDEzNjkwMDoyMjg4MzQ6MDoxNWQ4ZTA0NTZmMzJmMDU5MDZiZGI4NzFkYmYyMWU4MGI6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=543ac6e037bc2b694ab593792b40d0e6; user_status=0; utk=68a7cec4fcc497341637647204f94686;"
cookie = "ta_random_userid=5yqsx6jxak; WafStatus=0; cid=6d07b3804ba608a64d44b53b9531a7a91650871235; ComputerID=6d07b3804ba608a64d44b53b9531a7a91650871235; PHPSESSID=0a1c33ad4bdcb6901ca58a383612f311; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjYwNjQ4MTg3Ojo6MTQzMDEzNjkwMDoyMjk4MTM6MDoxMGIzZWUwMjQ2MzFiODk5MGQzOGNjZjAwYmEzMjViMjk6ZGVmYXVsdF80OjA=; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=552cc7fe963006bd667796d1be1b2408; user_status=0; utk=d2645ace2ac1fd46115ea4109454c7ca;"
cookie = "ta_random_userid=5yqsx6jxak; WafStatus=0; cid=6d07b3804ba608a64d44b53b9531a7a91650871235; ComputerID=6d07b3804ba608a64d44b53b9531a7a91650871235; PHPSESSID=0a1c33ad4bdcb6901ca58a383612f311; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjYwOTA3MjI1Ojo6MTQzMDEzNjkwMDo0MDI3NzU6MDoxMjUyNmJjZDU2ZDIxY2ZkNmI2NTlmYzEwOTJjN2U5YWU6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=030aefa0b3b3d7ff9f27db2b54141715; user_status=0; utk=f27680f08badd40f39ba37acbf46424f;"
cookie = "ta_random_userid=5yqsx6jxak; WafStatus=0; cid=6d07b3804ba608a64d44b53b9531a7a91650871235; ComputerID=6d07b3804ba608a64d44b53b9531a7a91650871235; PHPSESSID=0a1c33ad4bdcb6901ca58a383612f311; wencai_pc_version=0; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjYxMzM4ODc0Ojo6MTQzMDEzNjkwMDo0MDMxMjY6MDoxN2I4YTdhYjM2YmMwNTYyZTczNDE1MzgyOWU0OWE5NzI6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=2544b730d8e3c51634172292da1e1477; user_status=0; utk=d46c8fe1a902c8d40fbe9eb5eaf0380f;"
cookie = "ta_random_userid=5yqsx6jxak; WafStatus=0; cid=6d07b3804ba608a64d44b53b9531a7a91650871235; ComputerID=6d07b3804ba608a64d44b53b9531a7a91650871235; PHPSESSID=0a1c33ad4bdcb6901ca58a383612f311; other_uid=Ths_iwencai_Xuangu_cc2f5n5mrn3aluslmseghuc0pp5jfwlb; wencai_pc_version=0; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjY0NTIxNzE4Ojo6MTQzMDEzNjkwMDo2MDQ4MDA6MDoxNjZmM2IyMjc1MTg0ODgzMjMzMDM4YWU0YmYxM2MyNmY6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=5293ca25f92a8363287c5789a3f62890; user_status=0; utk=d15b33519175068f329ab5ac1269e7d5;"
cookie = "ta_random_userid=5yqsx6jxak; WafStatus=0; cid=6d07b3804ba608a64d44b53b9531a7a91650871235; ComputerID=6d07b3804ba608a64d44b53b9531a7a91650871235; PHPSESSID=0a1c33ad4bdcb6901ca58a383612f311; other_uid=Ths_iwencai_Xuangu_cc2f5n5mrn3aluslmseghuc0pp5jfwlb; wencai_pc_version=0; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjY1MjAwMDI4Ojo6MTQzMDEzNjkwMDozMTQ3NzI6MDoxNDcyYzc5ODYwZmZkNTJjY2JmNDAyZmFlMzY5MWQ0ZGM6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=208fed8eaec8442d1596d40b02700237; user_status=0; utk=735aa8c4027cbf096418418ebbd04bc1;"
cookie = "ta_random_userid=5yqsx6jxak; WafStatus=0; cid=6d07b3804ba608a64d44b53b9531a7a91650871235; ComputerID=6d07b3804ba608a64d44b53b9531a7a91650871235; PHPSESSID=0a1c33ad4bdcb6901ca58a383612f311; other_uid=Ths_iwencai_Xuangu_cc2f5n5mrn3aluslmseghuc0pp5jfwlb; wencai_pc_version=0; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjY1MjAwMDI4Ojo6MTQzMDEzNjkwMDozMTQ3NzI6MDoxNDcyYzc5ODYwZmZkNTJjY2JmNDAyZmFlMzY5MWQ0ZGM6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=208fed8eaec8442d1596d40b02700237; user_status=0; utk=735aa8c4027cbf096418418ebbd04bc1;"

def GetTHS_V():
    size = len(eng_10jqka_CookieList)
    index = random.randint(0,size-1)
    return eng_10jqka_CookieList[index]

def GetDailyData(dbConnection,logger):
    logger.error("开始获取每日收盘数据")
    v = GetTHS_V()
    dailyFetcher = CFetchDailyDataFromTHS(cookie,v)
    dailyFetcher.GetDailyData()
    basicSqls = dailyFetcher.FormateBacicInfoToSQL('stockBasicInfo') 
    for sql in basicSqls: 
        logger.info(sql)
        dbConnection.Execute(sql)
        
    dailySqls = dailyFetcher.FormateDailyInfoToSQL('stockDailyInfo') 
    for sql in dailySqls: 
        logger.info(sql)
        dbConnection.Execute(sql)


def GetNewHighData(dbConnection,logger):
    logger.error("开始获取每日新高数据")
    v = GetTHS_V()
    newHighFetcher = CNewHighDataFromTHS(cookie,v)
    newHighFetcher.GetNewHighData()

        
def GetZhangTingData(dbConnection,logger):
    logger.error("开始获取每日涨停数据")
    v = GetTHS_V()
    dailyFetcher = CFetchZhangTingDataFromTHS(cookie,v)
    dailyFetcher.GetZhangTingData()
    zhangTingSql = dailyFetcher.FormateZhangTingInfoToSQL('stockZhangting') 
    for sql in zhangTingSql:
        logger.info(sql)
        dbConnection.Execute(sql) 

def GetZhangTingLanBanData(dbConnection,logger):
    logger.error("开始获取每日涨停烂板数据")
    #烂板
    v = GetTHS_V()
    dailyFetcher = CFetchZhangTingLanBanFromTHS(cookie,v)
    dailyFetcher.GetZhangTingLanBanData()


def GetNoZhangTingLanBanData(dbConnection,logger):
    logger.error("开始获取烂长上影线数据")
    #上影线数据
    tradingDays = GetTradingDateLastN(dbConnection,15)
    dailyFetcher = CFetchNoZhangTingData(dbConnection,tradingDays[-1])
    dailyFetcher.FetchNoZhangTingData()
    


def GetLiangDaData(dbConnection,logger):
    logger.error("开始获取每日放大量数据")
    #成家量急剧放大
    v = GetTHS_V()
    dailyFetcher = CFetchDaLiangFromTHS(cookie,v)
    dailyFetcher.GetDaLiangData()

def GetLiangDaLanBanDaData(dbConnection,logger):
    logger.error("开始获取每日放大量数据并且是烂板数据")
    #成家量急剧放大 并且收了一个烂板
    v = GetTHS_V()
    dailyFetcher = CFetchDaLiangAndLanBanFromTHS(cookie,v)
    dailyFetcher.GetDaLiangLanBanData()


def Test(dbConnection):
    sql = 'select `股票代码`,`所属概念` from `stockBasicInfo`;'
    results,_ = dbConnection.Query(sql)
    all = {}
    for result in results:
        stockID,gainians = result
        gs = gainians.split(';')
        for g in gs:
            if g not in all:
                all[g] = []
            all[g].append(stockID)
    
    for a in all:
        print(a,all[a])
        
    print(all.keys())

def GetTHSData():
    logger = StartToInitLogger("同花顺日常数据")
    logger.info(f'==============begin:{datetime.datetime.now()}==============================')
    dbConnection = ConnectToDB()
    #GetDailyData(dbConnection,logger)
    #GetZhangTingData(dbConnection,logger)
    #GetNewHighData(dbConnection,logger)
    GetLiangDaData(dbConnection,logger)
    GetLiangDaLanBanDaData(dbConnection,logger)
    GetZhangTingLanBanData(dbConnection,logger)
    GetNoZhangTingLanBanData(dbConnection,logger)
    logger.info(f'==============end:{datetime.datetime.now()}==============================')
    
def AutoDownload():
    schedule.every().day.at("17:35").do(GetTHSData)
    while(True):
        schedule.run_pending()
        time.sleep(1)

def GenerateCookie():
    t = int(time.time())
    f = f"0:yuchonghuang::None:500:250679370:7,11111111111,40;44,11,40;6,1,40;5,1,40;1,101,40;2,1,40;3,1,40;5,1,40;8,00000000000000000000001,40;102,1,40:27:::240679370:{t}:::1430136900:402033:0:191af6e8a01ba174ff281a4bc021b29d5:default_4:0"
    s1 = base64.b64encode(f.encode("utf-8"))
    c = quote(s1)
    c= "MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjU2NTg3OTY3Ojo6MTQzMDEzNjkwMDo0MDIwMzM6MDoxOTFhZjZlOGEwMWJhMTc0ZmYyODFhNGJjMDIxYjI5ZDU6ZGVmYXVsdF80OjA%3D"
    cookie = f"ta_random_userid=5yqsx6jxak; WafStatus=0; cid=6d07b3804ba608a64d44b53b9531a7a91650871235; ComputerID=6d07b3804ba608a64d44b53b9531a7a91650871235; PHPSESSID=0a1c33ad4bdcb6901ca58a383612f311; user={c}; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=226af5baa728cc18e5276a51452d679d; user_status=0; utk=5f6e249a6fabe336236997a67d5a09d5;"
    print(cookie)
    return cookie


def GetZhangTingLanBanTest():
    logger = StartToInitLogger("test")
    v = GetTHS_V()
    dailyFetcher = CFetchZhangTingLanBanFromTHS(cookie,v)
    dailyFetcher.GetZhangTingLanBanData()
 
def GetDaliangTest():
    logger = StartToInitLogger("test")
    v = GetTHS_V()
    dailyFetcher = CFetchDaLiangFromTHS(cookie,v)
    dailyFetcher.GetDaLiangData()


def GetDaliangLanBanTest():
    logger = StartToInitLogger("test")
    v = GetTHS_V()
    dailyFetcher = CFetchDaLiangAndLanBanFromTHS(cookie,v)
    dailyFetcher.GetDaLiangLanBanData()

if __name__ == "__main__":
    #loggerAutoDownload()
    #cookie = GenerateCookie()
    GetTHSData()
    # GetDaliangLanBanTest()
    #GetDaliangTest()