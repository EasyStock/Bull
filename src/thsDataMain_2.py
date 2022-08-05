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



ZHANGTING_TABLE_NAME = 'zhangting'
ZHANGTING_REASON_TABLE_NAME = 'zhangtingreason'
# cookie ="cid=b699fcc5acb1074f7f421e52773a94171609932410; ComputerID=b699fcc5acb1074f7f421e52773a94171609932410; other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024; WafStatus=0; PHPSESSID=4ac00185ac3cad0742526448795429f4; ta_random_userid=30xnca4on3; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjQxNDYwNTA0Ojo6MTQzMDEzNjkwMDo0MDIyOTY6MDoxY2E5ODM3OGZmMTgzOTE3NDljYTdkNDIzNWEzNzcyZjQ6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=76ee766f86732421e91b32de69f6c352; user_status=0; utk=d7b2d63fb9ab284163fb4f014f6587c1;"
# cookie ="cid=b699fcc5acb1074f7f421e52773a94171609932410; ComputerID=b699fcc5acb1074f7f421e52773a94171609932410; other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024; WafStatus=0; PHPSESSID=4ac00185ac3cad0742526448795429f4; ta_random_userid=30xnca4on3; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjQxODk5NjkyOjo6MTQzMDEzNjkwMDoyMjk1MDg6MDoxOTJjMTcyNDFmNTU0YjdlM2M3NThkOGRmMzFmZmIwNmI6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=864169332df85a86b062f9d76a0a37c4; user_status=0; utk=76d227129f3aa4f2c538ada4f8ebb741;"



cookie = "ta_random_userid=5yqsx6jxak; WafStatus=0; cid=6d07b3804ba608a64d44b53b9531a7a91650871235; ComputerID=6d07b3804ba608a64d44b53b9531a7a91650871235; PHPSESSID=0a1c33ad4bdcb6901ca58a383612f311; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjU4MzY0MzgzOjo6MTQzMDEzNjkwMDo0MDA0MTc6MDoxMjRiZmQ1ZTc5ODBkNTU4MGIxN2JlNmYwMjlhMmQ1MjE6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=320dd44c4bb53749369a65274368f53a; user_status=0; utk=eeb0549bcf3de92c942f9b1183adb72b;"
cookie ="ta_random_userid=5yqsx6jxak; WafStatus=0; cid=6d07b3804ba608a64d44b53b9531a7a91650871235; ComputerID=6d07b3804ba608a64d44b53b9531a7a91650871235; PHPSESSID=0a1c33ad4bdcb6901ca58a383612f311; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjU5NTI0ODU3Ojo6MTQzMDEzNjkwMDo0MDI3NDM6MDoxZGRlNmYwNmY1M2FiZDlkMzI5NzM4ZjdiODYzNTJjNjY6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=240b5cb9cd8fc15fcf52bd3137d16c64; user_status=0; utk=0f3ed8c1f3707af24e84e247d5c81d41;"

def GetTHS_V():
    size = len(eng_10jqka_CookieList)
    index = random.randint(0,size-1)
    return eng_10jqka_CookieList[index]

def GetDailyData(dbConnection,logger):
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
    v = GetTHS_V()
    newHighFetcher = CNewHighDataFromTHS(cookie,v)
    newHighFetcher.GetNewHighData()

        
def GetZhangTingData(dbConnection,logger):
    v = GetTHS_V()
    dailyFetcher = CFetchZhangTingDataFromTHS(cookie,v)
    dailyFetcher.GetZhangTingData()
    zhangTingSql = dailyFetcher.FormateZhangTingInfoToSQL('stockZhangting') 
    for sql in zhangTingSql:
        logger.info(sql)
        dbConnection.Execute(sql) 

def GetZhangTingLanBanData(dbConnection,logger):
    #烂板
    v = GetTHS_V()
    dailyFetcher = CFetchZhangTingLanBanFromTHS(cookie,v)
    dailyFetcher.GetZhangTingLanBanData()

def GetLiangDaData(dbConnection,logger):
    #成家量急剧放大
    v = GetTHS_V()
    dailyFetcher = CFetchDaLiangFromTHS(cookie,v)
    dailyFetcher.GetDaLiangData()

def GetLiangDaLanBanDaData(dbConnection,logger):
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
    GetDailyData(dbConnection,logger)
    GetZhangTingData(dbConnection,logger)
    GetNewHighData(dbConnection,logger)
    GetLiangDaData(dbConnection,logger)
    GetLiangDaLanBanDaData(dbConnection,logger)
    GetZhangTingLanBanData(dbConnection,logger)
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