import requests
import pandas as pd

def GetRecording():
    headers = {
        'sec-ch-ua': 'Not A;Brand;v=99, Chromium;v=98, Google Chrome;v=98 ',
        'accept': 'application/json, text/plain, */*',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36',
        'sec-ch-ua-platform': 'macOS',
        'origin': 'https://live.jtzyuan.com',
        'sec-fetch-site': 'same-site',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://live.jtzyuan.com/',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    maxNone = 0
    start =10111933
    #start = 10067539
    result = []
    res = {}
    for i in range(5000):
        
        api = f'https://vod.jtzyuan.com/api/queryVideoById?id={start+i}'
        response = requests.get(api,headers = headers)
        if response.status_code == 200:
            j = response.json()
            if maxNone >=10:
                break
            
            if j['result'] is None:
                maxNone = maxNone + 1
                continue
            maxNone = 1
            index = f"https://live.jtzyuan.com/special/vod/?id={start+i}#/"
            url = j['result']["url"]
            name = j['result']["name"]
            data = [start+i,index,name,api,url]
            result.append(data)
            if "盯盘抓板" in name or "复盘寻龙" in name or "大师直播课" in name:  
                res[name] = index
                print(url) 
                #input()

            print(f"{i:<4d} :   {name}  {api}")
        else:
            print(response.text)
            
    df = pd.DataFrame(result,columns=["id","index","name","API","URL"])
    df.to_excel('/tmp/recording.xlsx',index=False)
    
    for key in res:
        print(key, res[key])

    

if __name__ == "__main__":
    GetRecording()