from mysql.connect2DB import ConnectToDB,ConnectToDB_AliYun_new
import pandas as pd

sqls = [
    '''CREATE DATABASE `stock`;''',
    '''USE `stock`; ''',
]

def _listAllTablesCreateStatementOfOneDatabase(dbConnection,databaseName):
    res = dbConnection.Execute(f''' USE  {databaseName}''')
    resultSQLS  = [
        f'''CREATE DATABASE `{databaseName}`;''',
        f'''USE `{databaseName}`; ''',
    ]
    if res :
        results, _  = dbConnection.Query("show tables;")
        tableNames = [f[0] for f in results]
        for tableName in tableNames:
            sql = f'''show create table `{tableName}`;'''
            res = dbConnection.Query(sql)
            if res is not None :
                resultSQLS.append(res[0][0][1]+";")
            else:
                sql1 = f'''show create view `{databaseName}`.`{tableName}`;'''
                res1 = dbConnection.Query(sql1)
                resultSQLS.append(res1[0][0][1]+";")
    
    return resultSQLS


def _backUpTables(oldDBConnection,dbConnection):
    exceptTables = ['information_schema','mysql','performance_schema','sys',"__recycle_bin__"]
    results, _  = oldDBConnection.Query('''show databases;''')
    datebaseNames = [f[0] for f in results]
    sqls = []
    for name in datebaseNames:
        if name in exceptTables:
            continue

        sqlList = _listAllTablesCreateStatementOfOneDatabase(oldDBConnection,name)
        sqls.extend(sqlList)
    
    with open("./backupTables.sql","w+") as f:
        for sql in sqls:
            f.write(sql)
            f.write("\n\n")
            print(sql)
        # res = dbConnection.Execute(sql)
        # if not res:
        #     print(sql)
        #     break

# def DataFrameToInsertSQLs(dataFrame,tableName):
#     sqls = []        
#     for _, row in dataFrame.iterrows():
#         indexStr = '''`,`'''.join(row.index)
#         valueStr = '''","'''.join(str(x) for x in row.values)
#         sql = f'''INSERT IGNORE INTO {tableName} ( `{indexStr}`)  VALUES ("{valueStr}");'''
#         sqls.append(sql)
#     return sqls
    
# def _backUpTableData(oldDBConnection,dbConnection,tableName):
#     data, columns  = oldDBConnection.Query(f'''SELECT * FROM {tableName};''')
#     df = pd.DataFrame(data,columns= columns)
#     sqls= DataFrameToInsertSQLs(df,tableName)
#     for sql in sqls:
#         #print(sql)
#         res = dbConnection.Execute(sql)
#         if not res:
#             print(sql)
#             break


# def BackupFromTeamsToLocal():
#     oldDBConnection = ConnectToDB_Teams()
#     newDB = ConnectToDB_local()
#     _backUpTables(oldDBConnection,newDB)

#     _backUpTableData(oldDBConnection,newDB,'''`Triage`.`VipInfo`''')
#     _backUpTableData(oldDBConnection,newDB,'''`TriageMC`.`VipInfo`''')


# def BackupFromMCToLocal():
#     oldDBConnection = ConnectToDB_WebexMeeting()
#     newDB = ConnectToDB_local()
#     _backUpTables( oldDBConnection,newDB)


# def _CreateUser(dbConnection):
#     userName = 'triager'
#     pwd = "pass1234"
#     sqls = [
#         f'''CREATE USER '{userName}'@'%' IDENTIFIED WITH mysql_native_password BY '{pwd}';''',
#         f'''GRANT ALL ON *.* TO '{userName}'@'%' WITH GRANT OPTION;'''
#     ]

#     for sql in sqls:
#         res = dbConnection.Execute(sql)
#         if not res:
#             print(sql)
#             break

def _MigrationData(oldDBConnection,dbConnection):
    exceptTables = ['information_schema','mysql','performance_schema','sys',"__recycle_bin__"]
    results, _  = oldDBConnection.Query('''show databases;''')
    datebaseNames = [f[0] for f in results]
    sqls = []
    for name in datebaseNames:
        if name in exceptTables:
            continue

        results, _  = oldDBConnection.Query("show tables;")
        tableNames = [f[0] for f in results]
        for tableName in tableNames:
            sql = f'''select * from `{tableName}`;'''
            print(sql)

def _VerifyData(oldDBConnection,dbConnection):
    exceptTables = ['information_schema','mysql','performance_schema','sys',"__recycle_bin__"]
    results, _  = oldDBConnection.Query('''show databases;''')
    datebaseNames = [f[0] for f in results]
    sqls = []
    for name in datebaseNames:
        if name in exceptTables:
            continue

        results, _  = oldDBConnection.Query("show tables;")
        tableNames = [f[0] for f in results]
        for tableName in tableNames:
            sql = f'''select count(*) from `{tableName}`;'''
            print(sql)
            results1, _  = oldDBConnection.Query(sql)
            results2, _  = dbConnection.Query(sql)
            if results1[0][0] != results2[0][0]:
                print("===========",tableName,"========\n")
            print(results1,results2)



def BackupFromAliyunToLocal():
    oldDBConnection = ConnectToDB()
    _backUpTables(oldDBConnection,None)

    # _backUpTableData(oldDBConnection,newDB,'''`Triage`.`VipInfo`''')
    # _backUpTableData(oldDBConnection,newDB,'''`TriageMC`.`VipInfo`''')
        

def MigrationData():
    oldDBConnection = ConnectToDB()
    newDBConnection = ConnectToDB_AliYun_new()
    _VerifyData(oldDBConnection,newDBConnection)

if __name__ == '__main__':
#    BackupFromAliyunToLocal()
#     #BackupFromMCToLocal()
#     #BackupFromMCToTeams()
    MigrationData()