
import os
import requests
import json


APP_ID="cli_a5d30a191678500e"
APP_SECRET="tuP2MBMCwH79NNvhsjfF5cvZcS1pxlnG"
VERIFICATION_TOKEN="RLrs0yF2KChC1xTRos7OydE4P1pqgehq"

LARK_HOST="https://open.feishu.cn"


# const
TENANT_ACCESS_TOKEN_URI = "/open-apis/auth/v3/tenant_access_token/internal"
MESSAGE_URI = "/open-apis/im/v1/messages"


def authorize_tenant_access_token():
    # get tenant_access_token and set, implemented based on Feishu open api capability. doc link: https://open.feishu.cn/document/ukTMukTMukTM/ukDNz4SO0MjL5QzM/auth-v3/auth/tenant_access_token_internal
    url = "{}{}".format(LARK_HOST, TENANT_ACCESS_TOKEN_URI)
    req_body = {"app_id": APP_ID, "app_secret": APP_SECRET}
    response = requests.post(url, req_body)
    _tenant_access_token = response.json().get("tenant_access_token")
    #print(_tenant_access_token)
    return _tenant_access_token

def sendMessage(tenant_access_token,receive_id_type, receive_id, msg_type, content):
    # send message to user, implemented based on Feishu open api capability. doc link: https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/im-v1/message/create
   
    url = "{}{}?receive_id_type={}".format(
        LARK_HOST, MESSAGE_URI, receive_id_type
    )
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + tenant_access_token,
    }

    req_body = {
        "receive_id": receive_id,
        "content":  content,
        "msg_type": msg_type,
    }
    resp = requests.post(url=url, headers=headers, json=req_body)
    if resp.status_code != 200:
        print(resp.text)

def formatCardOfFeishu(date,df):
    if df.empty:
        return None
    contents = []
    tag = {"tag":"hr"}
    for _, row in df.iterrows():
        reason = row["原因"].replace("\t"," ").replace("\u3000"," ")
        stockID = row["转债代码"]
        stockName = row["转债名称"]
        reason = f'''**<font color=red>{reason}</font>**'''
        s = f"**转债名称** : {stockName}\n**转债代码** : {stockID}\n**原        因** : {reason}"
        content = {"content":s,"tag":"markdown"}
        contents.append(content)
        contents.append(tag)
    t = f"可转债条件不符合预警-{date}"
    title = {"content":t,"tag":"plain_text"}
    ret = {"config":{"wide_screen_mode":True},"elements":contents, "header":{"template":"purple","title":title}}
    return ret


if __name__ == "__main__":
    receive_id_type = "chat_id"
    receive_id = "oc_10d43bbd79f9623a413f16a347ce7510"
    msg_type = "text"
    # content = "\"<b>bold content<i>, bold and italic content</i></b>\""
    # msgContent = {
    #     "text": content,
    # }
    t = {"config":{"wide_screen_mode":True},"elements":[{"content":"**转债名称** : 微糖股份\n**转债代码** : 0000311\n**原        因** : AAA\n","tag":"markdown"},{"tag":"hr"},{"content":"**转债名称** : A微糖股份\n**转债代码** : B0000311\n**原        因** : CAAA","tag":"markdown"},{"tag":"hr"}],"header":{"template":"purple","title":{"content":"可转债条件不符合预警-2022年11月1日","tag":"plain_text"}}}
    content = json.dumps(t,ensure_ascii=False)
    #print(content)
    msg_type = "interactive"
    # content = "{\"config\":{\"wide_screen_mode\":true},\"elements\":[{\"alt\":{\"content\":\"\",\"tag\":\"plain_text\"},\"img_key\":\"img_7ea74629-9191-4176-998c-2e603c9c5e8g\",\"tag\":\"img\"},{\"tag\":\"div\",\"text\":{\"content\":\"你是否曾因为一本书而产生心灵共振，开始感悟人生？\\n你有哪些想极力推荐给他人的珍藏书单？\\n\\n加入 **4·23 飞书读书节**，分享你的**挚爱书单**及**读书笔记**，**赢取千元读书礼**！\\n\\n📬 填写问卷，晒出你的珍藏好书\\n😍 想知道其他人都推荐了哪些好书？马上[入群围观](https://open.feishu.cn/)\\n📝 用[读书笔记模板](https://open.feishu.cn/)（桌面端打开），记录你的心得体会\\n🙌 更有惊喜特邀嘉宾 4月12日起带你共读\",\"tag\":\"lark_md\"}},{\"actions\":[{\"tag\":\"button\",\"text\":{\"content\":\"立即推荐好书\",\"tag\":\"plain_text\"},\"type\":\"primary\",\"url\":\"https://open.feishu.cn/\"},{\"tag\":\"button\",\"text\":{\"content\":\"查看活动指南\",\"tag\":\"plain_text\"},\"type\":\"default\",\"url\":\"https://open.feishu.cn/\"}],\"tag\":\"action\"}],\"header\":{\"template\":\"turquoise\",\"title\":{\"content\":\"📚晒挚爱好书，赢读书礼金\",\"tag\":\"plain_text\"}}}"
    # print(content)
    _tenant_access_token = authorize_tenant_access_token()
    #sendMessage(_tenant_access_token,receive_id_type,receive_id,msg_type,content)
