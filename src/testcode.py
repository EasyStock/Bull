import requests 
import json


getAccessTokenUrl = 'https://ft.10jqka.com.cn/api/v1/get_access_token' 
refreshToken = 'eyJzaWduX3RpbWUiOiIyMDIzLTA3LTEzIDIwOjA2OjU1In0=.eyJ1aWQiOiI2ODQwNzI5MzcifQ==.132A3FBAF5B8D569BD39E7E13E660CD647B8C955D8D03ED1FB28773972C6643A'
getAccessTokenHeader = {"Content- Type":"application/json","refresh_token":refreshToken} 
getAccessTokenResponse = requests.post(url=getAccessTokenUrl,headers=getAccessTokenHeader) 
print(getAccessTokenResponse.content)
accessToken = json.loads(getAccessTokenResponse.content)['data']['access_token'] 
print(accessToken)

# updateURL = "https://ft.10jqka.com.cn/api/v1/update_access_token"
# getAccessTokenResponse = requests.post(url=updateURL,headers=getAccessTokenHeader) 
# print(getAccessTokenResponse.content)
# requestMethod:POST/GET
# requestURL:https://quantapi.51ifind.com/api/v1/get_access_token
# requestHeaders:{"Content-Type":"application/json","refresh_token":""}


# refresh_token = "eyJzaWduX3RpbWUiOiIyMDIzLTA3LTEzIDIwOjA2OjU1In0=.eyJ1aWQiOiI2ODQwNzI5MzcifQ==.132A3FBAF5B8D569BD39E7E13E660CD647B8C955D8D03ED1FB28773972C6643A"

# thsUrl = 'https://ft.10jqka.com.cn/ds_service/api/v1/real_time_quotation' 
# #accessToken = 'eyJzaWduX3RpbWUiOiIyMDIzLTA3LTEzIDIwOjA2OjU1In0=.eyJ1aWQiOiI2ODQwNzI5MzcifQ==.132A3FBAF5B8D569BD39E7E13E660CD647B8C955D8D03ED1FB28773972C6643A' 
# thsHeaders = {"Content-Type":"application/json","access_token":accessToken} 
# thsPara = {"codes":"300033.SZ","indicators":"open,high,low,latest"} 
# thsResponse = requests.post(url=thsUrl,json=thsPara,headers=thsHeaders) 
# print(thsResponse.content)

thsUrl = 'https://ft.10jqka.com.cn/api/v1/snap_shot'
thsHeaders = {"Content-Type":"application/json","access_token":accessToken} 
thsPara = {"codes":"002526.SZ,300033.SZ","indicators":"bidSize1,askSize1,bidSize2,askSize2","starttime":"2023-07-13 09:15:00","endtime":"2023-07-13 09:25:00"} 
thsResponse = requests.post(url=thsUrl,json=thsPara,headers=thsHeaders)
t = json.loads(thsResponse.content)['tables']
print(thsResponse.content)
