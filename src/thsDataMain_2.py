from logging import log
from thsData.fetchDailyDataFromTHS import CFetchDailyDataFromTHS
from thsData.constants_10jqka import eng_10jqka_CookieList
import random
from mysql.mysql import CMySqlConnection
from thsData.fetchZhangTingFromTHS import CFetchZhangTingDataFromTHS
from mysql.connect2DB import ConnectToDB
from ColoredLog import StartToInitLogger
import schedule
import time
import datetime


ZHANGTING_TABLE_NAME = 'zhangting'
ZHANGTING_REASON_TABLE_NAME = 'zhangtingreason'
# cookie ="cid=b699fcc5acb1074f7f421e52773a94171609932410; ComputerID=b699fcc5acb1074f7f421e52773a94171609932410; other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024; WafStatus=0; PHPSESSID=4ac00185ac3cad0742526448795429f4; ta_random_userid=30xnca4on3; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjQxNDYwNTA0Ojo6MTQzMDEzNjkwMDo0MDIyOTY6MDoxY2E5ODM3OGZmMTgzOTE3NDljYTdkNDIzNWEzNzcyZjQ6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=76ee766f86732421e91b32de69f6c352; user_status=0; utk=d7b2d63fb9ab284163fb4f014f6587c1;"
# cookie ="cid=b699fcc5acb1074f7f421e52773a94171609932410; ComputerID=b699fcc5acb1074f7f421e52773a94171609932410; other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024; WafStatus=0; PHPSESSID=4ac00185ac3cad0742526448795429f4; ta_random_userid=30xnca4on3; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjQxODk5NjkyOjo6MTQzMDEzNjkwMDoyMjk1MDg6MDoxOTJjMTcyNDFmNTU0YjdlM2M3NThkOGRmMzFmZmIwNmI6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=864169332df85a86b062f9d76a0a37c4; user_status=0; utk=76d227129f3aa4f2c538ada4f8ebb741;"
# cookie = "cid=b699fcc5acb1074f7f421e52773a94171609932410; ComputerID=b699fcc5acb1074f7f421e52773a94171609932410; other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024; WafStatus=0; PHPSESSID=4ac00185ac3cad0742526448795429f4; ta_random_userid=30xnca4on3; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjQyMTU0MzI0Ojo6MTQzMDEzNjkwMDozOTk2NzY6MDoxMTZkYmU0MDYwODNjYWMxMWZkYThjYWVmMzg5Y2U4MDk6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=c253bd89a452468168af77444cfa9648; user_status=0; utk=55bf48d017477624f6522f0fa041a82c;"
# cookie = "cid=b699fcc5acb1074f7f421e52773a94171609932410; ComputerID=b699fcc5acb1074f7f421e52773a94171609932410; other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024; WafStatus=0; PHPSESSID=4ac00185ac3cad0742526448795429f4; ta_random_userid=30xnca4on3; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjQyNTgzODQxOjo6MTQzMDEzNjkwMDo0MDIxNTk6MDoxNDRhNzQ5OGFkZTNjOTYwMzJjNzcwMzcxZDcwNDlhNzk6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=3e60a6f998d3418ef40f4db8adceab6d; user_status=0; utk=6d23888ba83fd8c8736aac90bc0e3b39;"
# cookie = "cid=b699fcc5acb1074f7f421e52773a94171609932410; ComputerID=b699fcc5acb1074f7f421e52773a94171609932410; other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024; WafStatus=0; PHPSESSID=4ac00185ac3cad0742526448795429f4; ta_random_userid=30xnca4on3; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjQ0MTUwMzM2Ojo6MTQzMDEzNjkwMDoyMjg4NjQ6MDoxYTAyNmI5ZDIxYWUwZmEyN2YwZmY0ZjgwNDdhYTg2OGE6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=9d08b72a6ab5e191301b87e8467f1faa; user_status=0; utk=2ab81ef2809471e563dfa658d0974917;"
# cookie = "cid=b699fcc5acb1074f7f421e52773a94171609932410; ComputerID=b699fcc5acb1074f7f421e52773a94171609932410; other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024; WafStatus=0; PHPSESSID=4ac00185ac3cad0742526448795429f4; ta_random_userid=30xnca4on3; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjQ0Mzk1NzMzOjo6MTQzMDEzNjkwMDo0MDEwNjc6MDoxNTFmN2VmY2RlNzFlZGNmYmNhMDQwZWNlYzEzNWExYWU6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=ae40c15316009cd944e9d7d7980ab8a0; user_status=0; utk=302cd3b0cef175bacd53c4bff57ac6d5;"
#cookie = "cid=b699fcc5acb1074f7f421e52773a94171609932410; ComputerID=b699fcc5acb1074f7f421e52773a94171609932410; other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024; WafStatus=0; PHPSESSID=4ac00185ac3cad0742526448795429f4; ta_random_userid=30xnca4on3; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjQ0ODM1NDQ2Ojo6MTQzMDEzNjkwMDoyMjc3NTQ6MDoxMzY5NTZmN2U0NTM4NzI4NzBlZTNlNGZlODhmOTUxZWQ6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=85e69a612de18b0076286e17af192946; user_status=0; utk=b9c3dd38dda03086a9b3243db1d4fa79;"
# cookie = "cid=b699fcc5acb1074f7f421e52773a94171609932410; ComputerID=b699fcc5acb1074f7f421e52773a94171609932410; other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024; WafStatus=0; PHPSESSID=4ac00185ac3cad0742526448795429f4; ta_random_userid=30xnca4on3; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjQ2NjQ0MzIzOjo6MTQzMDEzNjkwMDoyMjk2Nzc6MDoxZDAxMGExOGRkZDQ4OTIyYTRkMTU4ZmM3OTU4MjY5OGY6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=e3a7d8c09a7480d66fa7a4f3ffa11cbf; user_status=0; utk=3cd9b3e6abb471431c839b5aa6404093;"

cookie = "cid=b699fcc5acb1074f7f421e52773a94171609932410; ComputerID=b699fcc5acb1074f7f421e52773a94171609932410; other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024;WafStatus=0; PHPSESSID=4ac00185ac3cad0742526448795429f4; ta_random_userid=30xnca4on3; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjQ4NDU1NTM5Ojo6MTQzMDEzNjkwMDoyMjkyNjE6MDoxMGI0ZWJmNWE1MDc2NTRjOWU3ZjJlY2U2NDJlOWRkOTk6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=3669bf23e92dd5da150652c736ae63cf; user_status=0; utk=afa36b98e2ba3b50f583d52f4e6b6895;"
cookie = "cid=b699fcc5acb1074f7f421e52773a94171609932410; ComputerID=b699fcc5acb1074f7f421e52773a94171609932410; other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024;WafStatus=0; PHPSESSID=4ac00185ac3cad0742526448795429f4; ta_random_userid=30xnca4on3; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjQ4NzI0MjU4Ojo6MTQzMDEzNjkwMDo1NzI1NDI6MDoxZmIyYWM0ZDMyMjM1NzUwMGU4NzgxOTRhMDBiYzgxYzE6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=90028f898d404ddc82bcc4043c40e6b8; user_status=0; utk=52ced193c99a1408a7553ab1a996fd5b;"
cookie = "cid=b699fcc5acb1074f7f421e52773a94171609932410; ComputerID=b699fcc5acb1074f7f421e52773a94171609932410; other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024;WafStatus=0; PHPSESSID=4ac00185ac3cad0742526448795429f4; ta_random_userid=30xnca4on3; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjQ5MzI1NTMyOjo6MTQzMDEzNjkwMDozOTk2Njg6MDoxMTNiYzQ0YzU1MWIwNDg4NDEzYjNmNzk0OTVhYmRjNmU6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=e32fa2ed60e7b75689f8c19f2982b9c9; user_status=0; utk=7ff902a04dfa25df1e8c6129d7ee40a3;"
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
        
def GetZhangTingData(dbConnection,logger):
    v = GetTHS_V()
    dailyFetcher = CFetchZhangTingDataFromTHS(cookie,v)
    dailyFetcher.GetZhangTingData()
    zhangTingSql = dailyFetcher.FormateZhangTingInfoToSQL('stockZhangting') 
    for sql in zhangTingSql:
        logger.info(sql)
        dbConnection.Execute(sql)    
    
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
    logger.info(f'==============end:{datetime.datetime.now()}==============================')
    
def AutoDownload():
    schedule.every().day.at("17:35").do(GetTHSData)
    while(True):
        schedule.run_pending()
        time.sleep(1)
    
if __name__ == "__main__":
    #loggerAutoDownload()
    GetTHSData()
