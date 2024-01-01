# **************************************
# --*-- coding: utf-8 --*--
# @Time    : 2022-12-07
# @Author  : white
# @FileName: 同花顺.py
# @Software: PyCharm
# **************************************
import execjs
import os

# 获取hexin-v
def get_hexin_v():
    folder = os.path.abspath(os.path.dirname(__file__))
    jsPath = os.path.join(folder,"hexin_v.js")
    #print(jsPath)
    hexin_v = execjs.compile(open(jsPath, encoding='utf-8').read()).call('white')
    #print(hexin_v)
    return hexin_v

if __name__ == '__main__':
    # os.environ['EXECJS_RUNTIME'] = 'Node'
    # node = execjs.get('node')
    # print(execjs.get().name)
    get_hexin_v()
