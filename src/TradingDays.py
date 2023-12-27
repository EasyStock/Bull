import calendar
from mysql.connect2DB import ConnectToDB

TRADING_DAY_EXCEPT = [
    "2024-01-01",
    "2024-02-09",
    "2024-02-10",
    "2024-02-11",
    "2024-02-12",
    "2024-02-13",
    "2024-02-14",
    "2024-02-15",
    "2024-02-16",
    "2024-02-17",
    "2024-02-18",
    "2024-04-04",
    "2024-04-05",
    "2024-04-06",
    "2024-04-07",
    "2024-05-01",
    "2024-05-02",
    "2024-05-03",
    "2024-05-04",
    "2024-05-05",
    "2024-06-08",
    "2024-06-09",
    "2024-06-10",
    "2024-09-14",
    "2024-09-15",
    "2024-09-16",
    "2024-09-17",
    "2024-10-01",
    "2024-10-02",
    "2024-10-03",
    "2024-10-04",
    "2024-10-05",
    "2024-10-06",
    "2024-10-07",
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
    days = GetTradingDays(2024)
    for day in days:
        sql  = f'''REPLACE INTO `stock`.`treadingday` (`交易所`, `日期`, `开市`) VALUES ('SSE', '{day[0]}', '{day[1]}');'''
        dbConnection.Execute(sql)

if __name__ == '__main__':
    dbconn = ConnectToDB()
    GetTradingDaysAndInSertToDB(dbconn,2024)