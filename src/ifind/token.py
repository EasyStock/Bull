

import requests 
import json
import datetime


class CIFindToken(object):
    def __init__(self,dbConnection = None) -> None:
        self.refreshToken = None
        self.access_token = None
        self.expired_time = None
        self.dbConnection = dbConnection

    def GetTokenInfoFromDB(self):
        sql = f'''SELECT * FROM stock.thstoken limit 1; '''
        res,_ = self.dbConnection.Query(sql)
        self.refreshToken,self.access_token,self.expired_time= res[0]

    def UpdateAccessToken(self):
        updateURL = "https://ft.10jqka.com.cn/api/v1/update_access_token"
        header = {"Content- Type":"application/json","refresh_token":self.refreshToken} 
        getAccessTokenResponse = requests.post(url=updateURL,headers=header) 
        print(getAccessTokenResponse.content)
        return getAccessTokenResponse.content

    def GetAccessTokenFromTHS(self):
        updateURL = "https://ft.10jqka.com.cn/api/v1/get_access_token"
        header = {"Content- Type":"application/json","refresh_token":self.refreshToken} 
        getAccessTokenResponse = requests.post(url=updateURL,headers=header) 
        print(getAccessTokenResponse.content)
        return getAccessTokenResponse.content

    def refreshAccessToken(self):
        if self.refreshToken is None:
            self.GetTokenInfoFromDB()
        
        msg = f"token: {self.access_token}, expired_time:{self.expired_time}"
        print(msg)
        if self.access_token is None or self.expired_time is None:
            res = self.GetAccessTokenFromTHS()
            tokenInfo = json.loads(res)['data']
            self.access_token = tokenInfo['access_token']
            self.expired_time =  tokenInfo["expired_time"]
            sql = f'''UPDATE `stock`.`thstoken` SET `access_token` = '{self.access_token}', `expired_time` = '{self.expired_time}' WHERE (`refreshToken` = '{self.refreshToken}');'''
            self.dbConnection.Execute(sql)
        else:
            d1 = datetime.datetime.strptime(str(self.expired_time), "%Y-%m-%d %H:%M:%S")
            now = datetime.datetime.utcnow()
            oneday=datetime.timedelta(days=3) 
            refreshTime=now+oneday
            if refreshTime > d1:
                res = self.UpdateAccessToken()
                tokenInfo = json.loads(res)['data']
                self.access_token = tokenInfo['access_token']
                self.expired_time =  tokenInfo["expired_time"]
                sql = f'''UPDATE `stock`.`thstoken` SET `access_token` = '{self.access_token}', `expired_time` = '{self.expired_time}' WHERE (`refreshToken` = '{self.refreshToken}');'''
                self.dbConnection.Execute(sql)

    