import tushare as ts 
import pandas as pd

ts.set_token('7c752594bc9e33aad08aec1742fc3e527e9fd60cedb67da440d6ca28')
pro = ts.pro_api()

stock_code = '000001'
start_date = '20190101' 
end_date = '20200101'

# 调用pro接口获取5档详细信息
df = pro.top_list(ts_code=stock_code, start_date=start_date, end_date=end_date)

# 保留需要的买卖5档列
df = df[['trade_date','buy_price1','buy_vol1','buy_price2','buy_vol2','buy_price3','buy_vol3','buy_price4','buy_vol4','buy_price5','buy_vol5',
         'sell_price1','sell_vol1','sell_price2','sell_vol2','sell_price3','sell_vol3','sell_price4','sell_vol4','sell_price5','sell_vol5']]

# 输出到CSV文件
df.to_csv('5bp_detail.csv', index=False)

print("5档盘口历史数据已保存到5bp_detail.csv")