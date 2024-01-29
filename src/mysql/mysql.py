#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   mysql.py
@Time    :   2021/06/05 21:38:00
@Author  :   Jianping Huang 
@Contact :   yuchonghuang@126.com
'''

import pymysql
from pymysql.constants import CLIENT

class CMySqlConnection(object):
    def __init__(self,host, port, user, password, dbName, charset = 'utf8'):
        print('connecting to mysql server...')
        self.db = pymysql.connect(host= host,port=port,user=user,password=password,db=dbName,charset=charset,client_flag = CLIENT.MULTI_STATEMENTS)
        print('conencted to mysql server successfully!') 
    
    def __del__(self):
        print('mysql server closed...') 
        if self.db:
            self.db.close()
    
        
    def _writeLogError(self,msg):
        if self.logger is None:
            return
        self.logger.error(msg)

    def Query(self,sql_statement):
        if self.db is None:
            print('query data failed, db is None.') 
            return
        
        cursor = self.db.cursor()
        try:
            cursor.execute(sql_statement)
            results = cursor.fetchall()
            columns = [item[0] for item in cursor.description]
            #print('query data successfully!') 
            return (results,columns)
        except:
            print('query data failed, Error: unable to fecth data')
            
    
    def Execute(self,sql_statement):
        if self.db is None:
            print('execule sql failed, db is None.') 
            return False
        
        cursor = self.db.cursor()
        try:
            cursor.execute(sql_statement)
            self.db.commit()
            return True
            #print('execule sql:[%s] successfully!'%(sql_statement))
        except Exception as e:
            print('execule sql:[%s] failed,Exception:[%s]'%(sql_statement,e))
            self.db.rollback()
            return False