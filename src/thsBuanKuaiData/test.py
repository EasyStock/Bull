
import requests
import json

def QueryHead(v):
    headers = {
    'Accept': 'application/json, text/plain, */*', #
    'Accept-Encoding': 'gzip, deflate', #
    'Accept-Language': 'zh-CN,zh;q=0.9', #
    'Cache-Control': 'no-cache', #
    'Connection': 'keep-alive', #
    'Content-Type': 'application/json', #
    'Content-Length': "499",
    'Host': 'www.iwencai.com',  
    'Origin': 'http://www.iwencai.com', #
    'Pragma': 'no-cache', #
    'Referer': 'http://www.iwencai.com',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko)', #
    "hexin-v":v, #
    "Cookie":"v=A3NVhL4aEoqqnd7eMwcUB1vVBH2YqI3DwVZJQCcSzWbJHJ1irXiXutEM2jI2; ComputerID=5c92ccfc1a14e6657fefbb052ad501331700525048; WafStatus=0; cid=5c92ccfc1a14e6657fefbb052ad501331700525048; @#!rsa_version!#@=default_4; @#!sessionid!#@=1103eddfe0e68a1ccfbb58efef915a2c7; @#!userid!#@=240679370; escapename=yuchonghuang; ticket=5d2483e2b60f103bb46e022068799e9a; u_name=yuchonghuang; user=MDp5dWNob25naHVhbmc6Ok5vbmU6NTAwOjI1MDY3OTM3MDo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoyNzo6OjI0MDY3OTM3MDoxNzAzODEyNjAwOjo6MTQzMDEzNjkwMDoyNjc4NDAwOjA6MWEyMGUyMjFkZmE4NjRmZGIzMzljMTcyMmVjZWMyMWM1Ojow; userid=240679370; PHPSESSID=d653ccea00fdcafb942f6de924045fb7; ta_random_userid=xnvrtloyju"
    }
    return headers


def Test():
    headers = QueryHead("A3NVhL4aEoqqnd7eMwcUB1vVBH2YqI3DwVZJQCcSzWbJHJ1irXiXutEM2jI2")
    url = "http://www.iwencai.com/customized/chart/get-robot-data"
    payload = {
	"source": "Ths_iwencai_Xuangu",
	"version": "2.0",
	"query_area": "",
	"block_list": "",
	"add_info": "{\"urp\":{\"scene\":1,\"company\":1,\"business\":1},\"contentType\":\"json\",\"searchInfo\":true}",
	"question": "2023.12.21 板块指数代码 指数简称 开盘价  收盘价 成交量 成交额 换手率 上涨家数 下跌家数 量比 流通市值 总市值 涨跌幅 流通市值 总市值",
	"perpage": 100,
	"page": 1,
	"secondary_intent": "zhishu",
	"log_info": "{\"input_type\":\"typewrite\"}",
	"rsh": "240679370"
    }
    response = requests.request("POST",url,headers=headers, data= json.dumps(payload))
    js =json.loads(response.text)
    status_code = js['status_code']
    datas = js['data']
    dd = datas['answer'][0]["txt"][0]["content"]["components"][0]["data"]
    datas = dd["datas"]
    meta = dd["meta"]["extra"]["row_count"]
    print(meta)
    for d in datas:
        print(d)
    return js


if __name__ == "__main__":
    Test()