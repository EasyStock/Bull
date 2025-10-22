from mysql.connect2DB import ConnectToDB,ConnectToDB_AliYun_new,ConnectToDB_Local
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
                sql1 = sql1.replace("jianpinh","root")
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


def DataFrameToInsertSQLs(dataFrame,tableName):
    sqls = []        
    for _, row in dataFrame.iterrows():
        indexStr = '''`,`'''.join(row.index)
        valueStr = '''","'''.join(str(x).replace("'", "\\'").replace('"', '\\"') for x in row.values)
        sql = f'''INSERT IGNORE INTO {tableName} ( `{indexStr}`)  VALUES ("{valueStr}");'''
        sqls.append(sql)
    return sqls

###################################################################
def _backUpTableData(oldDBConnection,dbConnection,tableName):
    print(f"ready to backup the data of {tableName}")
    data, columns  = oldDBConnection.Query(f'''SELECT * FROM {tableName};''')
    df = pd.DataFrame(data,columns= columns)
    sqls= DataFrameToInsertSQLs(df,tableName)
    step = 300
    groupedSql = [" ".join(sqls[i:i+step]) for i in range(0,len(sqls),step)]
    for sql in groupedSql:
        res = dbConnection.Execute(sql)
        if not res:
            print(sql)
            raise Exception("backUpTableData error")


def BackUpTables(oldDBConnection,dbConnection):
    exceptTables = ['information_schema','mysql','performance_schema','sys']
    results, _  = oldDBConnection.Query('''show databases;''')
    datebaseNames = [f[0] for f in results]
    sqls = []
    for name in datebaseNames:
        if name in exceptTables:
            continue

        sqlList = _listAllTablesCreateStatementOfOneDatabase(oldDBConnection,name)
        sqls.extend(sqlList)
    
    step = 300
    groupedSql = [" ".join(sqls[i:i+step]) for i in range(0,len(sqls),step)]
    for sql in groupedSql:
        res = dbConnection.Execute(sql)
        if not res:
            print(sql)
            raise Exception("BackUpTables error")
        

def BackUpDatas(oldDBConnection,dbConnection):
    exceptTables = ['information_schema','mysql','performance_schema','sys','__recycle_bin__']
    results, _  = oldDBConnection.Query('''show databases;''')
    datebaseNames = [f[0] for f in results]
    for name in datebaseNames:
        if name in exceptTables:
            continue

        dbConnection.Execute(f''' USE `{name}`; ''')
        results, _  = dbConnection.Query(f'''show tables;''')
        tableNames = [f[0] for f in results]
        for tableName in tableNames:
            fullTableName = f'''`{name}`.`{tableName}`'''
            _backUpTableData(oldDBConnection,dbConnection,fullTableName)
            print(fullTableName)



def BackUp():
    connFrom = ConnectToDB_AliYun_new()
    connTo = ConnectToDB_Local()
    #BackUpTables(connFrom,connTo)
    BackUpDatas(connFrom,connTo)





################################################################################################
class CDBMigration(object):
    def __init__(self):
        pass

    staticmethod
    def GetDatabases(dbConnection):
        exceptTables = ['information_schema','mysql','performance_schema','sys',"__recycle_bin__"]
        results, _  = dbConnection.Query('''show databases;''')
        datebaseNames = [f[0] for f in results if f[0] not in exceptTables]
        return datebaseNames
    
    
    def GetTablesAndViews(dbConnection,databaseName):
        res = dbConnection.Execute(f''' USE  {databaseName}''')
        ret  = {
            "tableNames":[],
            "viewNames":[],
            "crateTableStatements":[],
            "createViewStatements":[]

        }
        if res :
            results, _  = dbConnection.Query("show tables;")
            tableNames = [f[0] for f in results]
            for tableName in tableNames:
                sql = f'''show create table `{tableName}`;'''
                res = dbConnection.Query(sql)
                if res is not None:
                    if res[1][0] in ("Table","table"):
                        fullTableName = f'''`{databaseName}`.`{tableName}`'''
                        createStatement = res[0][0][1]+";"
                        createStatement = createStatement.replace("CREATE TABLE","CREATE TABLE IF NOT EXISTS")
                        ret["tableNames"].append(fullTableName)
                        ret["crateTableStatements"].append(createStatement)
                    else:
                        sql1 = f'''show create view `{databaseName}`.`{tableName}`;'''
                        res1 = dbConnection.Query(sql1)
                        fullTableName = f'''`{databaseName}`.`{tableName}`'''
                        statement = res1[0][0][1]+";"
                        statement = statement.replace("CREATE","CREATE OR REPLACE")
                        statement = statement.replace("jianpinh","root")
                        ret["viewNames"].append(fullTableName)
                        ret["createViewStatements"].append(statement)
        
        return ret
    
    @staticmethod
    def CreateDataBase(dbConnectionTo,datebaseName):
        sqls = [
            f'''CREATE DATABASE IF NOT EXISTS `{datebaseName}`;''',
            f'''USE `{datebaseName}`; ''',
        ]

        for sql in sqls:
            dbConnectionTo.Execute(sql)

    @staticmethod
    def CreateTables(dbConnectionTo,tablesAndViews):
        crateTableStatements = tablesAndViews["crateTableStatements"]
        createViewStatements = tablesAndViews["createViewStatements"]
        CDBMigration.ExecSqls(dbConnectionTo,crateTableStatements)
        CDBMigration.ExecSqls(dbConnectionTo,createViewStatements)

    
    @staticmethod
    def BackupData(dbConnectionFrom,dbConnectionTo,tableName):
        print(f"ready to backup the data of {tableName}")
        data, columns  = dbConnectionFrom.Query(f'''SELECT * FROM {tableName};''')
        df = pd.DataFrame(data,columns= columns)
        sqls= DataFrameToInsertSQLs(df,tableName)
        dbConnectionTo.Execute(f'''TRUNCATE {tableName};''')
        step = 800
        groupedSql = [" ".join(sqls[i:i+step]) for i in range(0,len(sqls),step)]
        for sql in groupedSql:
            res = dbConnectionTo.Execute(sql)
            if not res:
                print(sql)
                raise Exception("backUpTableData error")
                
    @staticmethod
    def VerifyData(dbConnectionFrom,dbConnectionTo,tableName):
        print(f"ready to verify the data of {tableName}")
        data1, _  = dbConnectionFrom.Query(f'''SELECT count(*) FROM {tableName};''')
        data2, _  = dbConnectionTo.Query(f'''SELECT count(*) FROM {tableName};''')
        if data1[0][0] != data2[0][0]:
            print(f"VerifyData failed,{tableName}, {data1[0][0]} VS {data2[0][0]}")
            return False
        
        return True
    
    @staticmethod
    def MirgateOneTable(dbConnectionFrom,dbConnectionTo,tableName):
        res = CDBMigration.VerifyData(dbConnectionFrom,dbConnectionTo,tableName)
        if res == False:
            CDBMigration.BackupData(dbConnectionFrom,dbConnectionTo,tableName)
            res1 = CDBMigration.VerifyData(dbConnectionFrom,dbConnectionTo,tableName)
            if res1 == False:
                raise Exception(f"VerifyData failed,{tableName}")
        else:
            print(f"{tableName} Mirgate already Done!")

        print(f"{tableName} Mirgate Done!")

    @staticmethod
    def Mirgate(dbConnectionFrom,dbConnectionTo,tableNames):
        for tableName in tableNames:
            CDBMigration.MirgateOneTable(dbConnectionFrom,dbConnectionTo,tableName)

    @staticmethod
    def ExecSqls(dbConnection,sqlStatements):
        for sql in sqlStatements:
            dbConnection.Execute(sql)

    @staticmethod
    def Migration(dbConnectionFrom,dbConnectionTo):   
        datebaseNames = CDBMigration.GetDatabases(dbConnectionFrom)
        for datebaseName in datebaseNames:
            CDBMigration.CreateDataBase(dbConnectionTo=dbConnectionTo,datebaseName=datebaseName)
            res = CDBMigration.GetTablesAndViews(dbConnectionFrom,datebaseName)
            CDBMigration.CreateTables(dbConnectionTo,res)
            tableNames = res["tableNames"]
            print(tableNames)
            CDBMigration.Mirgate(dbConnectionFrom,dbConnectionTo,tableNames)
            


if __name__ == '__main__':
#    BackupFromAliyunToLocal()
#     #BackupFromMCToLocal()
#     #BackupFromMCToTeams()
    #MigrationData()
    #BackUp()
    dbConnectionFrom = ConnectToDB_AliYun_new()
    dbConnectionTo = ConnectToDB_Local()
    CDBMigration.Migration(dbConnectionFrom,dbConnectionTo)
    #CDBMigration.MirgateOneTable(dbConnectionFrom,dbConnectionTo,'`stock`.`stockdailyinfo_2024`' )