from mysql.connect2DB import ConnectToDB
import pandas as pd

def Test1_BuyTogether(dbConnection,operatorID1, operatorID2):
    sql = f'''select * from `stock`.`dragon` where (operator_ID = {operatorID1} or operator_ID = {operatorID2})  and `flag` = "B" and (date,stockID) in (select date,stockID from `stock`.`dragon` where operator_ID = {operatorID2} and `flag` = "B" and (sell = "nan" or sell = 0) and (date,stockID) in (select date,stockID from `stock`.`dragon` where operator_ID = {operatorID1} and `flag` = "B" and (sell = "nan" or sell = 0)))
'''
    print(sql)
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    print(df)

def Test1_SellTogether(dbConnection,operatorID1, operatorID2):
    sql = f'''select * from `stock`.`dragon` where (operator_ID = {operatorID1} or operator_ID = {operatorID2})  and `flag` = "S" and (date,stockID) in (select date,stockID from `stock`.`dragon` where operator_ID = {operatorID2} and `flag` = "S" and (buy = "nan" or buy = 0) and (date,stockID) in (select date,stockID from `stock`.`dragon` where operator_ID = {operatorID1} and `flag` = "S" and (buy = "nan" or buy = 0)))
'''
    print(sql)
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    print(df)




if __name__ == "__main__":
    dbConnection = ConnectToDB()
    Test1_BuyTogether(dbConnection,10028451,10656871)
    Test1_SellTogether(dbConnection,10028451,10656871)