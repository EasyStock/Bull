# 9. 正股打分信息
# 10. 板块打分信息

from mysql.connect2DB import ConnectToDB
from fupan.tradingDate import GetTradingDateLastN


def _Validata(dbConnection,sql,threshold):
    results, _ = dbConnection.Query(sql)
    count = results[0][0]
    if count < threshold:
        raise Exception("_Validata Data Failed!")

    return True

def _Validata1(dbConnection,sql,threshold):
    results, _ = dbConnection.Query(sql)
    count = results[0][0]
    if count >= threshold:
        raise Exception("_Validata Data Failed!")

    return True

def ValidateBasicInfo(dbConnection,tradingDays):
    # 1. 正股的基础信息
    sql = f'''SELECT count(*) FROM stock.stockbasicinfo where `更新日期` = "{tradingDays[-1]}";'''
    return _Validata(dbConnection,sql,5000)

def ValidateDailyInfo(dbConnection,tradingDays):
    # 2. 正股每日信息
    sql = f'''SELECT count(*) FROM stock.stockdailyinfo where `日期` = "{tradingDays[-1]}";'''
    return _Validata(dbConnection,sql,5000)

def ValidateIndexInfo(dbConnection,tradingDays):
    # 3. 指数信息
    sql = f'''SELECT count(*) FROM stock.kaipanla_index where `date` = "{tradingDays[-1]}";'''
    return _Validata(dbConnection,sql,4)

def ValidateZhangTingInfo(dbConnection,tradingDays):
    # 4. 涨停信息
    sql = f'''SELECT count(*) FROM stock.stockzhangting where `日期` = "{tradingDays[-1]}";'''
    return _Validata(dbConnection,sql,1)

def ValidateFuPanInfo(dbConnection,tradingDays):
    # 5. 市场情绪分析结果
    sql = f'''SELECT count(*) FROM stock.fupan where `日期` = "{tradingDays[-1]}";'''
    return _Validata(dbConnection,sql,1)

def ValidateZhuanZaiInfo(dbConnection,tradingDays):
    # 6. 可转债信息
    sql = f'''SELECT count(*) FROM stock.kezhuanzhai where `日期` = "{tradingDays[-1]}";'''
    _Validata(dbConnection,sql,50)

    sql1 = f'''SELECT count(*) FROM stock.kezhuanzhai_all where `日期` = "{tradingDays[-1]}";'''
    _Validata(dbConnection,sql1,200)

def ValidateZhuanZaiCompareIndexInfo(dbConnection,tradingDays):
    # 7. 可转债对比指数分数
    sql = f'''SELECT count(*) FROM stock.compareindex_zai where `date` = "{tradingDays[-1]}";'''
    _Validata(dbConnection,sql,1)

    sql1 = f'''SELECT count(*) FROM stock.compareindex_zai where `date` = "{tradingDays[-1]}" and flag is NULL;'''
    _Validata1(dbConnection,sql1,1)

def ValidateBanKuaiInfo(dbConnection,tradingDays):
    # 8. 板块信息
    sql = f'''SELECT count(*) FROM stock.bankuai_index_dailyinfo where `日期` = "{tradingDays[-1]}";'''
    return _Validata(dbConnection,sql,700)


def Validate_ALL(dbConnection,tradingDays):
    ValidateBasicInfo(dbConnection,tradingDays)
    ValidateDailyInfo(dbConnection,tradingDays)
    ValidateIndexInfo(dbConnection,tradingDays)
    ValidateZhangTingInfo(dbConnection,tradingDays)
    ValidateFuPanInfo(dbConnection,tradingDays)
    ValidateZhuanZaiInfo(dbConnection,tradingDays)
    ValidateZhuanZaiCompareIndexInfo(dbConnection,tradingDays)
    ValidateBanKuaiInfo(dbConnection,tradingDays)

if __name__ == "__main__":
    dbConnection = ConnectToDB()
    tradingDays = GetTradingDateLastN(dbConnection,15)
    Validate_ALL(dbConnection,tradingDays)


