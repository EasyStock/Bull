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

# cookie = "cid=b699fcc5acb1074f7f421e52773a94171609932410; ComputerID=b699fcc5acb1074f7f421e52773a94171609932410; other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024;WafStatus=0; PHPSESSID=4ac00185ac3cad0742526448795429f4; ta_random_userid=30xnca4on3; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjQ4NDU1NTM5Ojo6MTQzMDEzNjkwMDoyMjkyNjE6MDoxMGI0ZWJmNWE1MDc2NTRjOWU3ZjJlY2U2NDJlOWRkOTk6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=3669bf23e92dd5da150652c736ae63cf; user_status=0; utk=afa36b98e2ba3b50f583d52f4e6b6895;"
# cookie = "cid=b699fcc5acb1074f7f421e52773a94171609932410; ComputerID=b699fcc5acb1074f7f421e52773a94171609932410; other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024;WafStatus=0; PHPSESSID=4ac00185ac3cad0742526448795429f4; ta_random_userid=30xnca4on3; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjQ4NzI0MjU4Ojo6MTQzMDEzNjkwMDo1NzI1NDI6MDoxZmIyYWM0ZDMyMjM1NzUwMGU4NzgxOTRhMDBiYzgxYzE6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=90028f898d404ddc82bcc4043c40e6b8; user_status=0; utk=52ced193c99a1408a7553ab1a996fd5b;"
# cookie = "cid=b699fcc5acb1074f7f421e52773a94171609932410; ComputerID=b699fcc5acb1074f7f421e52773a94171609932410; other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024;WafStatus=0; PHPSESSID=4ac00185ac3cad0742526448795429f4; ta_random_userid=30xnca4on3; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjQ5MzI1NTMyOjo6MTQzMDEzNjkwMDozOTk2Njg6MDoxMTNiYzQ0YzU1MWIwNDg4NDEzYjNmNzk0OTVhYmRjNmU6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=e32fa2ed60e7b75689f8c19f2982b9c9; user_status=0; utk=7ff902a04dfa25df1e8c6129d7ee40a3;"
# cookie= "cid=b699fcc5acb1074f7f421e52773a94171609932410; ComputerID=b699fcc5acb1074f7f421e52773a94171609932410; other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024;WafStatus=0; ta_random_userid=30xnca4on3; PHPSESSID=e055cf526a3e846708b7d8a2d05c67b8; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjQ5NzQ4ODE3Ojo6MTQzMDEzNjkwMDoyMjgzODM6MDoxZjIyMDkzMDQ1NmMxZjgyZTRkZDZmM2U3MDIwOTI1MjQ6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=0b260141788e8926d643254e31ccd097; user_status=0; utk=3328e048adaad2548edb31c082412940;"
# cookie = "cid=b699fcc5acb1074f7f421e52773a94171609932410; ComputerID=b699fcc5acb1074f7f421e52773a94171609932410; other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024;WafStatus=0; ta_random_userid=30xnca4on3; PHPSESSID=e055cf526a3e846708b7d8a2d05c67b8; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjUwMDEwMzk4Ojo6MTQzMDEzNjkwMDo0MDI0MDI6MDoxNThiOWQzZDk5Yjg0ZWRiNzdlNGFjNDdlMWQwYmMxOTk6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=13fce3a9439c047478eb26d3fee57efe; user_status=0; utk=d4122ce369a2e068a5350b42093f4da1;"
# cookie = "other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024; WafStatus=0; PHPSESSID=dbe5028c17909f74d45b987a8d97f9b9; cid=dbe5028c17909f74d45b987a8d97f9b91650454503; ComputerID=dbe5028c17909f74d45b987a8d97f9b91650454503; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjUwNDU0NTUxOjo6MTQzMDEzNjkwMDo0MDEwNDk6MDoxN2JhOTQxOTAwODJkZTYzZmFjYzk2NmJmM2ZkZjA0ZWE6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=952528df2764f40b2f7c42685df78d82; user_status=0; utk=e19c0e6974a035621522209582972276;"
# cookie = "other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024;  WafStatus=0; PHPSESSID=6d07b3804ba608a64d44b53b9531a7a9; cid=6d07b3804ba608a64d44b53b9531a7a91650871235; ComputerID=6d07b3804ba608a64d44b53b9531a7a91650871235; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjUwODcxMjU0Ojo6MTQzMDEzNjkwMDoyMjkxNDY6MDoxMWNlZjU1ZTFjNjNiODljOTZjNTJiYjdkOTI3ZTBhNmU6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=b264deb4a85fd3fa87a06fae0a2d6b01; user_status=0; utk=f2e1c3b76b32c6f10bcebeb29b78e630;"
cookie = "other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024;  WafStatus=0; PHPSESSID=6d07b3804ba608a64d44b53b9531a7a9; cid=6d07b3804ba608a64d44b53b9531a7a91650871235; ComputerID=6d07b3804ba608a64d44b53b9531a7a91650871235; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjUxMTQ1NDMyOjo6MTQzMDEzNjkwMDo2MDQ4MDA6MDoxMWFkZDkyYjU4NTQ4MmUyMGU2ZmY1MTY0Y2VlMWY4NmI6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=f42d6eac4c60718bb7aeb36c55d03791; user_status=0; utk=0ebb98098e286d0baf37557d51781af3;"
cookie = "other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024;  WafStatus=0; PHPSESSID=6d07b3804ba608a64d44b53b9531a7a9; cid=6d07b3804ba608a64d44b53b9531a7a91650871235; ComputerID=6d07b3804ba608a64d44b53b9531a7a91650871235; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjUxODI0MDc2Ojo6MTQzMDEzNjkwMDo0MDMxMjQ6MDoxY2Y2ZTMwYTE1Mzc1YWYxNDUxMGZkYTk5ODdmOGY0MWI6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=06d5ba1ae2cddc7ba95a03aa3eeb0d61; user_status=0; utk=ee2d6cd458a835c0101591aab0e97b4c;"
cookie = "other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024; WafStatus=0; PHPSESSID=6d07b3804ba608a64d44b53b9531a7a9; cid=6d07b3804ba608a64d44b53b9531a7a91650871235; ComputerID=6d07b3804ba608a64d44b53b9531a7a91650871235; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjUyMjU2OTMzOjo6MTQzMDEzNjkwMDo0MDIyNjc6MDoxZWVkMWM0OGI0M2U2NDY4YTZlOGIyZDQ3ZGQxMDQ1NDY6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=3bfe890d1fec96b578cfa2cc8eb50340; user_status=0; utk=70161d414ea29c243fd0346f61707e4b;"
cookie = "other_uid=Ths_iwencai_Xuangu_e98244ff8bb1daadaa5ffbd956a40024; WafStatus=0; PHPSESSID=6d07b3804ba608a64d44b53b9531a7a9; cid=6d07b3804ba608a64d44b53b9531a7a91650871235; ComputerID=6d07b3804ba608a64d44b53b9531a7a91650871235; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNjUyNjg2NTI1Ojo6MTQzMDEzNjkwMDoyMjgyNzU6MDoxZjg3YWE2MmI2NjhlYTM2Y2ZlOWI5ZTU0MDMzY2U2ZmU6ZGVmYXVsdF80OjA%3D; userid=240679370; u_name=yuchonghuang; escapename=yuchonghuang; ticket=25ab40220e91a15c7003d5d39e328b7a; user_status=0; utk=7ce3be27b599677787341e3e234c1047;"


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
