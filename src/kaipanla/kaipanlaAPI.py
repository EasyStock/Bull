import requests
import json




class CkaiPanLaApi(object):
    def __init__(self,url,queryStr,host) -> None:
        self.url = url
        self.queryStr = queryStr
        self.host = host
        self.context_type = 'application/x-www-form-urlencoded; charset=utf-8'
        self.connection = 'close'
        self.accept = '*/*'
        self.userAgent = 'lhb/5.7.15 (com.kaipanla.www; build:0; iOS 15.4.0) Alamofire/5.7.15'
        self.acceptLanguage = 'zh-Hans-CN;q=1.0, en-CN;q=0.9'
        self.acceptEncoding = 'gzip;q=1.0, compress;q=0.5'


    def RequstData(self):
        #url = "https://apphis.longhuvip.com/w1/api/index.php"
        headers = {
        'Host': self.host, 
        'Content-Type': self.context_type,
        'Connection': self.connection,
        'Accept': self.accept,
        'User-Agent': self.userAgent,
        'Accept-Language': self.acceptLanguage,
        'Content-Length': str(len(self.queryStr)),
        'Accept-Encoding': self.acceptEncoding,
        }
        response = requests.request("POST", self.url, headers=headers, data=self.queryStr,verify=False)
        #print(response.text)
        return response.text





def RequstData():
    url = "https://apphis.longhuvip.com/w1/api/index.php"
    queryString = "Day=2023-04-21&Filter=0&FilterGem=0&FilterMotherboard=0&FilterTIB=0&Index=0&Is_st=1&Order=1&PhoneOSNew=2&PidType=2&Type=4&VerSion=5.7.0.15&a=HisDaBanList&apiv=w31&c=HisHomeDingPan&st=20"
                  
    #queryString =  "Filter=0&FilterGem=0&FilterMotherboard=0&FilterTIB=0&Index=0&Is_st=1&Order=1&PhoneOSNew=2&PidType=2&Type=4&VerSion=5.6.0.1&a=DaBanList&apiv=w30&c=HomeDingPan&st=20"
    headers = {
    'Host': 'apphis.longhuvip.com', 
    'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8',
    'Connection': 'close',
    'Accept': '*/*',
    'User-Agent': 'lhb/5.7.15 (com.kaipanla.www; build:0; iOS 15.4.0) Alamofire/5.7.15',
    'Accept-Language': 'zh-Hans-CN;q=1.0, en-CN;q=0.9',
    'Content-Length': str(len(queryString)),
    'Accept-Encoding': 'gzip;q=1.0, compress;q=0.5',

    }
    response = requests.request("POST", url, headers=headers, data=queryString,verify=False)
    #js =json.loads(response.text)
    print(response.text)


def RequstData11(): #炸板
    url = "https://apphq.longhuvip.com/w1/api/index.php"
    queryString = "Filter=0&FilterGem=0&FilterMotherboard=0&FilterTIB=0&Index=0&Is_st=1&Order=1&PhoneOSNew=2&PidType=2&Type=4&VerSion=5.6.0.1&a=DaBanList&apiv=w30&c=HomeDingPan&st=20"
    headers = {
    'Host': 'apphq.longhuvip.com', 
    'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8',
    'Connection': 'close',
    'Accept': '*/*',
    'User-Agent': 'lhb/5.7.15 (com.kaipanla.www; build:0; iOS 15.4.0) Alamofire/5.7.15',
    'Accept-Language': 'zh-Hans-CN;q=1.0',
    'Content-Length': str(len(queryString)),
    'Accept-Encoding': 'gzip;q=1.0, compress;q=0.5',

    }
    response = requests.request("POST", url, headers=headers, data=queryString,verify=False)
    #js =json.loads(response.text)
    print(response.text)


def RequstData2222(): #市场量能
    url = "https://apphq.longhuvip.com/w1/api/index.php"
    queryString = "PhoneOSNew=2&Type=0&VerSion=5.7.0.15&a=MarketCapacity&apiv=w31&c=HomeDingPan"
                  
    #queryString =  "Filter=0&FilterGem=0&FilterMotherboard=0&FilterTIB=0&Index=0&Is_st=1&Order=1&PhoneOSNew=2&PidType=2&Type=4&VerSion=5.6.0.1&a=DaBanList&apiv=w30&c=HomeDingPan&st=20"
    headers = {
    'Host': 'apphq.longhuvip.com', 
    'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8',
    'Connection': 'close',
    'Accept': '*/*',
    'User-Agent': 'lhb/5.7.15 (com.kaipanla.www; build:0; iOS 15.4.0) Alamofire/5.7.15',
    'Accept-Language': 'zh-Hans-CN;q=1.0, en-CN;q=0.9',
    'Content-Length': str(len(queryString)),
    'Accept-Encoding': 'gzip;q=1.0, compress;q=0.5',

    }
    response = requests.request("POST", url, headers=headers, data=queryString,verify=False)
    #js =json.loads(response.text)
    print(response.text)

def RequstData3333(): #市场量能历史
    url = "https://apphis.longhuvip.com/w1/api/index.php"
    queryString = "Date=2023-04-27&PhoneOSNew=2&Type=0&VerSion=5.9.0.3&a=MarketCapacity&apiv=w32&c=HisHomeDingPan"
                  
    headers = {
    'Host': 'apphis.longhuvip.com', 
    'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8',
    'Connection': 'close',
    'Accept': '*/*',
    'User-Agent': 'lhb/5.7.15 (com.kaipanla.www; build:0; iOS 15.4.0) Alamofire/5.7.15',
    'Accept-Language': 'zh-Hans-CN;q=1.0, en-CN;q=0.9',
    'Content-Length': str(len(queryString)),
    'Accept-Encoding': 'gzip;q=1.0, compress;q=0.5',

    }
    response = requests.request("POST", url, headers=headers, data=queryString,verify=False)
    #js =json.loads(response.text)
    print(response.text)


if __name__ == "__main__":
    RequstData3333()


