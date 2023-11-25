
import requests
import hashlib
import base64
import hmac
import time


def gen_sign(timestamp, secret):
    # 拼接timestamp和secret
    string_to_sign = '{}\n{}'.format(timestamp, secret)
    hmac_code = hmac.new(string_to_sign.encode("utf-8"), digestmod=hashlib.sha256).digest()
    # 对结果进行base64处理
    sign = base64.b64encode(hmac_code).decode('utf-8')
    return sign


def sendMessageByWebhook(webhook, secret, msg_type, content):
    # send message to user, implemented based on Feishu open api capability. doc link: https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/im-v1/message/create
    time_now = int(time.time())
    sign = gen_sign(time_now,secret)
    headers = {
        "Content-Type": "application/json"
    }

    req_body = {
        "timestamp": f'''{time_now}''',
        "card":  content,
        "msg_type": msg_type,
        "sign": sign,
    }
    resp = requests.post(url=webhook, headers=headers, json=req_body)
    print(resp.text)

if __name__ == "__main__":
    webhook = "https://open.feishu.cn/open-apis/bot/v2/hook/4901573e-b858-434a-a787-5faa28982b1a"
    secret = "brYyzPbSks4OKnMgdwKvIh"
    message = {
                "text": "request example"
        }
    sendMessageByWebhook(webhook,secret,"text",message)
