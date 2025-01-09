import calendar
from mysql.connect2DB import ConnectToDB

TRADING_DAY_EXCEPT = [
    "2025-01-01",

    "2025-01-28",
    "2025-01-29",
    "2025-01-30",
    "2025-01-31",
    "2025-02-01",
    "2025-02-02",
    "2025-02-03",
    "2025-02-04",

    "2025-04-04",
    "2025-04-05",
    "2025-04-06",

    "2025-05-01",
    "2025-05-02",
    "2025-05-03",
    "2025-05-04",
    "2025-05-05",

    "2025-05-31",
    "2025-06-01",
    "2025-06-02",

    "2025-10-01",
    "2025-10-02",
    "2025-10-03",
    "2025-10-04",
    "2025-10-05",
    "2025-10-06",
    "2025-10-07",
    "2025-10-08",
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
    GetTradingDaysAndInSertToDB(dbconn,2025)