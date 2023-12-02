from mysql.mysql import CMySqlConnection

def ConnectToDB_Local():
    dbName = 'stock'
    host = '127.0.0.1'
    port =3306
    user = 'stock'
    password= 'Stock@1234'
    connect = CMySqlConnection(host,port,user, password, dbName)
    return connect

def ConnectToDB():
    #return ConnectToDB_Local()
    return ConnectToDB_AliYun()

def ConnectToDB_AliYun():
    pass


def DataFrameToSqls_INSERT_OR_IGNORE(datas,tableName):
    sqls = []
    for _, row in datas.iterrows():
        index_str = '''`,`'''.join(row.index)
        value_str = '''","'''.join(str(x) for x in row.values)
        sql = '''INSERT IGNORE INTO {0} (`{1}`) VALUES ("{2}");'''.format(tableName,index_str,value_str)
        sqls.append(sql)
    return sqls

def DataFrameToSqls_REPLACE(datas,tableName):
    sqls = []
    for _, row in datas.iterrows():
        index_str = '''`,`'''.join(row.index)
        value_str = '''","'''.join(str(x) for x in row.values)
        sql = '''REPLACE INTO `{0}` (`{1}`) VALUES ("{2}");'''.format(tableName,index_str,value_str)
        sqls.append(sql)
    return sqls

def DataFrameToSqls_UPDATE(datas,tableName,index_str):
    sqls = []
    for index, row in datas.iterrows():
        sql = '''UPDATE %s SET ''' %(tableName)

        for rowIndex, value in row.items():
            sql = sql + '''`%s` = '%s',''' %(rowIndex,value)
        sql = sql[:-1]
        sql = sql + ''' WHERE `%s` = '%s'; '''%(index_str,index)
        sqls.append(sql)
    return sqls