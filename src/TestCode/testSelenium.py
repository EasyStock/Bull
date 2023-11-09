from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time


# chrome_options = Options() 
# chrome_options.add_argument('window-size=1920,3000')  # 指定浏览器分辨率
# chrome_options.add_argument('--disable-gpu')  # 谷歌文档提到需要加上这个属性来规避bug
# chrome_options.add_argument('--hide-scrollbars')  # 隐藏滚动条, 应对一些特殊页面
# chrome_options.add_argument('blink-settings=imagesEnabled=false')  # 不加载图片, 提升速度
# chrome_options.add_argument("--disable-blink-features=AutomationControlled")  # 不被检测到是测试环境
# chrome_options.add_argument('--headless')  # 浏览器不提供可视化页面. linux下如果系统不支持可视化不加这条会启动失败
# chrome_options.add_argument('--no-sandbox')
# chrome_options.binary_location = "/usr/bin/google-chrome" 
# chrome_options.add_argument("--start-maximized") #open Browser in maximized mode
# chrome_options.add_argument("--disable-dev-shm-usage") #overcome limited resource problems
# chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
# chrome_options.add_experimental_option('useAutomationExtension', False)

# browser = webdriver.Chrome(options=chrome_options,executable_path="/usr/bin/chromedriver")
# browser.get('https://www.baidu.com/')
# time.sleep(3)
# print(browser.get_cookies())
# browser.quit()

options = webdriver.FirefoxOptions()
options.add_argument("--headless")
browser = webdriver.Firefox(options=options)

browser.get('https://www.baidu.com/')
time.sleep(3)
print(browser.get_cookies())
browser.quit()