from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time


class CJiSiLuSelenium(object):
    def __init__(self) -> None:
        pass


    def formatCookie(self,dbConnections = None):
        chrome_options = Options() 
        chrome_options.add_argument('window-size=1920,3000')  # 指定浏览器分辨率
        chrome_options.add_argument('--disable-gpu')  # 谷歌文档提到需要加上这个属性来规避bug
        chrome_options.add_argument('--hide-scrollbars')  # 隐藏滚动条, 应对一些特殊页面
        chrome_options.add_argument('blink-settings=imagesEnabled=false')  # 不加载图片, 提升速度
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")  # 不被检测到是测试环境
        chrome_options.add_argument('--headless')  # 浏览器不提供可视化页面. linux下如果系统不支持可视化不加这条会启动失败

        browser = webdriver.Chrome(options=chrome_options)
        browser.get('https://www.jisilu.cn/account/login/')
        name =  browser.find_element(By.XPATH, '/html/body/div[3]/div/div/div[1]/div[1]/div[3]/form/div[2]/input')
        name.send_keys('15336546675')
        password =  browser.find_element(By.XPATH, '/html/body/div[3]/div/div/div[1]/div[1]/div[3]/form/div[3]/input')
        password.send_keys('Mll1999052')
        term =  browser.find_element(By.XPATH, '/html/body/div[3]/div/div/div[1]/div[1]/div[3]/form/div[5]/div[2]/span/span')
        term.click()

        login =  browser.find_element(By.XPATH, '/html/body/div[3]/div/div/div[1]/div[1]/div[3]/form/div[6]/a')
        login.click()
        time.sleep(3)
        cookies = browser.get_cookies()
        print(cookies)
        #self.cookie = f"kbz_newcookie=1; kbzw__Session=9aqo0c4fm6dh30cv9lgck4n1l1; Hm_lvt_164fe01b1433a19b507595a43bf58262=1698406312; kbzw__user_login={self.kbzw_user_login}; Hm_lpvt_164fe01b1433a19b507595a43bf58262=1698412256"
        keys = ["kbz_newcookie","kbzw__Session","Hm_lvt_164fe01b1433a19b507595a43bf58262","kbzw__user_login","Hm_lpvt_164fe01b1433a19b507595a43bf58262"]
        result = ""
        for cookie in cookies:
            name = cookie["name"]
            value = cookie["value"]
            if name in keys:
                result = result + f"{name}={value}; "
        browser.quit()
        print("cookie:",result)
        if dbConnections is not None:
            sql = f'''UPDATE `stock`.`cookies` SET `name` = 'jisilu', `cookie` = '{result}' WHERE (`name` = 'jisilu');'''
            dbConnections.Execute(sql)
        return result


# c = CJiSiLuSelenium()
# c.formatCookie()
