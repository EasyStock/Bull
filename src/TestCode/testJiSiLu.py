import urllib.request

import ssl
ssl._create_default_https_context = ssl._create_unverified_context

url = "https://www.jisilu.cn/account/login/"
user = "15336546675"
pwd = "Mll1999052"
pwdMgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
pwdMgr.add_password(None,url,user,pwd)
auth_handler = urllib.request.HTTPBasicAuthHandler(pwdMgr)
opener = urllib.request.build_opener(auth_handler)
response = opener.open(url)
context = response.read().decode('utf-8')

with open("/tmp/aa.html",'w+') as f:
    f.write(context)