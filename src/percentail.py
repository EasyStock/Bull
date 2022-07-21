from mysql.connect2DB import ConnectToDB
import pandas as pd



def CalcPercentail():
    pd.set_option('display.unicode.ambiguous_as_wide',True)
    pd.set_option('display.unicode.east_asian_width',True)
    pd.set_option('display.width',360)
    dbConnection = ConnectToDB()
    sql = '''SELECT * FROM stock.fuPan;'''
    data, columns = dbConnection.Query(sql)
    df = pd.DataFrame(data=data,columns=columns)
    newColunms = ["红盘","两市量","实际涨停","跌停","炸板","炸板率","连板","10CM首板奖励率","10CM连板奖励率","首板个数","2连板个数","3连板个数","4连板及以上个数","高度板","动能","势能"]
    newDF = pd.DataFrame(df,columns=newColunms)
    newDF['炸板率'] = newDF['炸板率'].apply(lambda x:x[:-1]).astype(float)
    newDF['两市量'] = newDF['两市量'].apply(lambda x:x[:-1]).astype(float)

    for c in newColunms:
        newDF[c] = newDF[c].astype(float)

    t = newDF.quantile([0.02, 0.05, 0.1, 0.5,0.9, 0.95, 0.98])
    
    print(t)
    t.to_excel('/tmp/Percentail1.xlsx')

    newColunms1 = ["首板率","连板率","昨日首板溢价率","昨日首板晋级率","昨日2板溢价率","昨日2板晋级率","涨停数量","连板数量","收-5数量","大盘红盘比","亏钱效应","首板红盘比","首板大面比","连板股的红盘比","连板比例","连板大面比","昨日连板未涨停数的绿盘比","势能EX","动能EX"]
    newDF2 = pd.DataFrame(df,columns=newColunms1)
    s = newDF2.quantile([0.02, 0.05, 0.1, 0.5,0.9, 0.95, 0.98])
    print(s)
    s.to_excel('/tmp/Percentail2.xlsx')

if __name__ == "__main__":
    CalcPercentail()
