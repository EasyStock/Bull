import calendar
from mysql.connect2DB import ConnectToDB

TRADING_DAY_EXCEPT = [
    # 元旦
    "2026-01-01",
    "2026-01-02",
    "2026-01-03",

    #春节
    "2026-02-15",
    "2026-02-16",
    "2026-02-17",
    "2026-02-18",
    "2026-02-19",
    "2026-02-20",
    "2026-02-21",
    "2026-02-22",
    "2026-02-23",

    #清明节
    "2026-04-04",
    "2026-04-05",
    "2026-04-06",

    #劳动节
    "2026-05-01",
    "2026-05-02",
    "2026-05-03",
    "2026-05-04",
    "2026-05-05",

    #端午节
    "2026-06-19",
    "2026-06-20",
    "2026-06-21",

    #中秋节
    "2026-09-25",
    "2026-09-26",
    "2026-09-27",

    #国庆节
    "2026-10-01",
    "2026-10-02",
    "2026-10-03",
    "2026-10-04",
    "2026-10-05",
    "2026-10-06",
    "2026-10-07",

]

def GetAllDays(year):
    result = []
    cal = calendar.Calendar()
    all = cal.yeardatescalendar(year)
    for quarter in all:
        for month in quarter:
            for weekday in month:
                for day in weekday:
                    t = day.weekday()
                    workday = [0,1,2,3,4]
                    result.append((str(day),"1" if t in workday else "0" ))
    return result


def GetTradingDays(year):
    result  = []
    allDays = GetAllDays(year)
    for day in allDays:
        if day[0] in TRADING_DAY_EXCEPT:
            result.append((str(day[0]),"0"))
        else:
            result.append(day)
    return result

def GetTradingDaysAndInSertToDB(dbConnection,year):
    days = GetTradingDays(year)
    for day in days:
        sql  = f'''REPLACE INTO `stock`.`treadingday` (`交易所`, `日期`, `开市`) VALUES ('SSE', '{day[0]}', '{day[1]}');'''
        dbConnection.Execute(sql)

if __name__ == '__main__':
    dbconn = ConnectToDB()
    GetTradingDaysAndInSertToDB(dbconn,2026)