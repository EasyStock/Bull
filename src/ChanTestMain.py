# 第三方函数库
import numpy as np
import pandas as pd
import datetime
import matplotlib as mpl
import matplotlib.pyplot as plt
# import matplotlib.fina as mpf
import mplfinance as mpf
import matplotlib.patches as patches
import talib


def adjust_by_cintainment(k_data):
    # 
    # 判断k线的包含关系，便于寻找顶分型和底分型
    # 输入：k线序列
    # 输出：adjusted_k_data, 处理后的k线序列
    trend = [0]
    adjusted_k_data = pd.DataFrame()
    temp_data = k_data[:1]
    
    for i in range(len(k_data)):
        
        last = temp_data.iloc[-1]
        is_equal = temp_data.iloc[-1]["high"] == k_data.high[i] and temp_data.iloc[-1]["low"] == k_data.low[i]  # 第1根等于第2根
        
        # 向右包含
        if temp_data.iloc[-1]["high"] >= k_data.high[i] and temp_data.iloc[-1]["low"] <= k_data.low[i] and not is_equal:
            if trend[-1] == -1:
                temp_data.iloc[-1]["high"] = k_data.high[i]
            else:
                temp_data.iloc[-1]["low"] = k_data.low[i]
        
        # 向左包含
        elif temp_data.iloc[-1]["high"] <= k_data.high[i] and temp_data.iloc[-1]["low"] >= k_data.low[i] and not is_equal:
            if trend[-1] == -1:
                temp_data.iloc[-1]["low"] = k_data.low[i]
            else:
                temp_data.iloc[-1]["high"] = k_data.high[i]
                
        elif is_equal:
            trend.append(0)
        
        elif temp_data.iloc[-1]["high"] > k_data.high[i] and temp_data.iloc[-1]["low"] > k_data.low[i]:
            trend.append(-1)
            temp_data = k_data[i:i+1]
            
        elif temp_data.iloc[-1]["high"] < k_data.high[i] and temp_data.iloc[-1]["low"] < k_data.low[i]:
            trend.append(1)
            temp_data = k_data[i:i+1]
        
        # 调整收盘价和开盘价
        if temp_data.iloc[-1]["open"] > temp_data.iloc[-1]["close"]:
            if temp_data.iloc[-1]["open"] > temp_data.iloc[-1]["high"]:
                temp_data.iloc[-1]["open"] = temp_data.iloc[-1]["high"]
            if temp_data.iloc[-1]["close"] < temp_data.iloc[-1]["low"]:
                 temp_data.iloc[-1]["close"] = temp_data.iloc[-1]["low"]
        else:
            if temp_data.iloc[-1]["open"] < temp_data.iloc[-1]["low"]:
                temp_data.iloc[-1]["open"] = temp_data.iloc[-1]["low"]
            if temp_data.iloc[-1]["close"] > temp_data.iloc[-1]["high"]:
                 temp_data.iloc[-1]["close"] = temp_data.iloc[-1]["high"]
        
        adjusted_data = k_data[i:i+1]
        adjusted_data.open[-1] = temp_data.iloc[-1]["open"]
        adjusted_data.close[-1] = temp_data.iloc[-1]["close"]
        adjusted_data.high[-1] = temp_data.iloc[-1]["high"]
        adjusted_data.low[-1] = temp_data.iloc[-1]["low"]
        adjusted_k_data = pd.concat([adjusted_k_data,adjusted_data],axis = 0)
    
    return adjusted_k_data



帮我优化下面的代码
def get_fx(adjusted_k_data):
    #
    # 寻找顶分型和底分型
    # 1）连续分型选择最极端值
    # 2）分型之间保证3根k线
    # 输入：调整后的k线序列
    # 输出：顶分型和底分型的位置

    temp_num = 0 #上一个顶或底的位置
    temp_high = 0 #上一个顶的high值
    temp_low = 0 #上一个底的low值
    temp_type = 0 # 上一个记录位置的类型
    
    fx_type = [] # 记录分型点的类型，1为顶分型，-1为底分型
    fx_time = [] # 记录分型点的时间
    fx_plot = [] # 记录点的数值，为顶分型取high值，为底分型取low值
    fx_data = pd.DataFrame() # 记录分型
    fx_offset = []
    
    # 加上线段起点
    fx_type.append(0)
    fx_offset.append(0)
    fx_time.append(adjusted_k_data.index[0].strftime("%Y-%m-%d %H:%M:%S"))
    fx_data = pd.concat([fx_data,adjusted_k_data[:1]],axis = 0)
    fx_plot.append((adjusted_k_data.low[0]+adjusted_k_data.high[0])/2)
    
    i = 1
    while (i < len(adjusted_k_data)-1):
        
        top = adjusted_k_data.high[i-1] <= adjusted_k_data.high[i] \
              and adjusted_k_data.high[i] > adjusted_k_data.high[i+1] #顶分型
        bottom = adjusted_k_data.low[i-1] >= adjusted_k_data.low[i] \
              and adjusted_k_data.low[i] < adjusted_k_data.low[i+1] #底分型
        
        if top:
            if temp_type == 1: 
                # 如果上一个分型为顶分型，则进行比较，选取高点更高的分型 
                if adjusted_k_data.high[i] <= temp_high:
                    i += 1
                else:
                    temp_high = adjusted_k_data.high[i]
                    temp_low = adjusted_k_data.low[i]
                    temp_num = i
                    temp_type = 1
                    i += 2 # 两个分型之间至少有3根k线
            elif temp_type == -1: 
                # 如果上一个分型为底分型，则记录上一个分型，用当前分型与后面的分型比较，选取同向更极端的分型
                if temp_low >= adjusted_k_data.high[i]:
                    # 如果上一个底分型的底比当前顶分型的顶高，则跳过当前顶分型。
                    i += 1
                else:
                    fx_type.append(-1)
                    fx_time.append(adjusted_k_data.index[temp_num].strftime("%Y-%m-%d %H:%M:%S"))
                    fx_data = pd.concat([fx_data,adjusted_k_data[temp_num:temp_num+1]],axis = 0)
                    fx_plot.append(temp_low)
                    fx_offset.append(temp_num)
                    temp_high = adjusted_k_data.high[i]
                    temp_low = adjusted_k_data.low[i]
                    temp_num = i
                    temp_type = 1
                    i += 2 # 两个分型之间至少有3根k线
            else:
                temp_high = adjusted_k_data.high[i]
                temp_low = adjusted_k_data.low[i]
                temp_num = i
                temp_type = 1
                i += 2

        elif bottom:
            if temp_type == -1: 
                # 如果上一个分型为底分型，则进行比较，选取低点更低的分型 
                if adjusted_k_data.low[i] >= temp_low:
                    i += 1
                else:
                    temp_low = adjusted_k_data.low[i]
                    temp_high = adjusted_k_data.high[i]
                    temp_num = i
                    temp_type = -1
                    i += 3
            elif temp_type == 1: 
                # 如果上一个分型为顶分型，则记录上一个分型，用当前分型与后面的分型比较，选取同向更极端的分型
                if temp_high <= adjusted_k_data.low[i]: 
                    # 如果上一个顶分型的底比当前底分型的底低，则跳过当前底分型。
                    i += 1
                else:
                    fx_type.append(1)
                    fx_time.append(adjusted_k_data.index[temp_num].strftime("%Y-%m-%d %H:%M:%S"))
                    fx_data = pd.concat([fx_data,adjusted_k_data[temp_num:temp_num+1]],axis = 0)
                    fx_plot.append(temp_high)
                    fx_offset.append(temp_num)
                    temp_low = adjusted_k_data.low[i]
                    temp_high = adjusted_k_data.high[i]
                    temp_num = i
                    temp_type = -1
                    i += 2
            else:
                temp_low = adjusted_k_data.low[i]
                temp_high = adjusted_k_data.high[i]
                temp_num = i
                temp_type = -1
                i += 2
        else:
            i += 1
    
    # 加上最后一个分型（上面的循环中最后的一个分型并未处理）
    if temp_type == -1:        
        fx_type.append(-1)
        fx_time.append(adjusted_k_data.index[temp_num].strftime("%Y-%m-%d %H:%M:%S"))
        fx_data = pd.concat([fx_data,adjusted_k_data[temp_num:temp_num+1]],axis = 0)
        fx_plot.append(temp_low)
        fx_offset.append(temp_num)
    elif temp_type == 1:
        fx_type.append(1)
        fx_time.append(adjusted_k_data.index[temp_num].strftime("%Y-%m-%d %H:%M:%S"))
        fx_data = pd.concat([fx_data,adjusted_k_data[temp_num:temp_num+1]],axis = 0)
        fx_plot.append(temp_high)
        fx_offset.append(temp_num)
    
    # 加上线段终点
    fx_type.append(0)
    fx_offset.append(len(adjusted_k_data)-1)
    fx_time.append(adjusted_k_data.index[-1].strftime("%Y-%m-%d %H:%M:%S"))
    fx_data = pd.concat([fx_data,adjusted_k_data[-1:]],axis = 0)
    fx_plot.append((adjusted_k_data.low[-1]+adjusted_k_data.high[-1])/2)
            
    return fx_type, fx_time, fx_data, fx_plot, fx_offset



def get_pivot(fx_plot, fx_offset, fx_observe):
    #
    # 计算最近的中枢
    # 注意：一个中枢至少有三笔
    # fx_plot 笔的节点股价
    # fx_offset 笔的节点时间点（偏移）
    # fx_observe 所观测的分型点
    
    if fx_observe < 1:
        # 处理边界
        right_bound = 0
        left_bount = 0
        min_high = 0
        max_low = 0
        pivot_x_interval = [left_bount,right_bound]
        pivot_price_interval = [max_low, min_high]
        return pivot_x_interval, pivot_price_interval
    
    right_bound = (fx_offset[fx_observe]+fx_offset[fx_observe-1])/2
    # 右边界是所观察分型的上一笔中位
    left_bount = 0
    min_high = 0
    max_low = 0
    
    if fx_plot[fx_observe] >= fx_plot[fx_observe-1]:
        # 所观察分型的上一笔是往上的一笔
        min_high = fx_plot[fx_observe]
        max_low = fx_plot[fx_observe-1]
    else: # 所观察分型的上一笔是往下的一笔
        max_low = fx_plot[fx_observe]
        min_high = fx_plot[fx_observe-1]
    
    i = fx_observe - 1
    cover = 0 # 记录走势的重叠区，至少为3才能画中枢
    while (i >= 1):
        if fx_plot[i] >= fx_plot[i-1]:
            # 往上的一笔
            if fx_plot[i] < max_low or fx_plot[i-1] > min_high:
                # 已经没有重叠区域了
                left_bount = (fx_offset[i] + fx_offset[i+1])/2
                break
            else:
                # 有重叠区域
                # 计算更窄的中枢价格区间
                cover += 1
                min_high = min(fx_plot[i], min_high)
                max_low = max(fx_plot[i-1], max_low)
        
        elif fx_plot[i] < fx_plot[i-1]:
            # 往下的一笔
            if fx_plot[i] > min_high or fx_plot[i-1] < max_low:
                # 已经没有重叠区域了
                left_bount = (fx_offset[i] + fx_offset[i+1])/2
                break
            else:
                # 有重叠区域
                # 计算更窄的中枢价格区间
                cover += 3
                min_high = min(fx_plot[i-1], min_high)
                max_low = max(fx_plot[i], max_low)     
        i -= 1
    
    
    if cover < 3:
        # 不满足中枢定义
        right_bound = 0
        left_bount = 0
        min_high = 0
        max_low = 0
          
    pivot_x_interval = [left_bount,right_bound]
    pivot_price_interval = [max_low, min_high]
    return pivot_x_interval, pivot_price_interval


def plot_k_series(k_data):
    # 画k线
    num_of_ticks = len(k_data)
    fig, ax = plt.subplots(figsize = (num_of_ticks,20))
    fig.subplots_adjust(bottom=0.2)
    dates = k_data.index
    # print dates
    ax.set_xticks(np.linspace(1,num_of_ticks,num_of_ticks))
    ax.set_xticklabels(list(dates))

    k_data_dict = {
        'Open': list(k_data.open),
        'High': list(k_data.high),
        'Low': list(k_data.low),
        'Close': list(k_data.close)
    }

    date_index = pd.date_range(start='2025-01-01', periods=len(k_data.open), freq='D')
    data = pd.DataFrame(k_data_dict, index=date_index)


    mpf.plot(data, type='candle')
    # mpf.candlestick2(
    #     ax, 
    #     list(k_data.open),list(k_data.close),list(k_data.high),list(k_data.low), 
    #     width=0.6, colorup='r', colordown='b',alpha=0.75 
    # )
    plt.grid(True)
    plt.setp(plt.gca().get_xticklabels(), rotation=30) 
    return ax

def plot_lines(ax, fx_plot, fx_offset):
    # 绘制笔和线段
    # ax 绘图区域
    # fx_plot
    plt.plot(fx_offset, fx_plot, 'k', lw=1)
    plt.plot(fx_offset, fx_plot, 'o')


def plot_pivot(ax, pivot_date_interval, pivot_price_interval):
    #
    # 绘制中枢
    start_point = (pivot_date_interval[0], pivot_price_interval[0])
    width = pivot_date_interval[1] - pivot_date_interval[0]
    height =  pivot_price_interval[1] - pivot_price_interval[0]
    ax.add_patch(
        patches.Rectangle(
            start_point,    # (x,y)
            width,          # width
            height,         # height
            linewidth=8,
            edgecolor='g',
            facecolor='none'
        )
    )
    return


file = "/Users/mac/Desktop/上证指数.csv"
k_series = pd.read_csv(file)


# start_date = datetime.datetime(2017, 11, 6, 9, 0, 0)
# end_date = datetime.datetime(2017, 11, 10, 15, 30, 0)
# k_series = get_k_series('000100.XSHE', start_date, end_date, 30)
#plot_k_series(k_series) # 原始k线图

adjusted_k_data = adjust_by_cintainment(k_series)
ax = plot_k_series(adjusted_k_data) # 调整后的k线图

fx_type, fx_time, fx_data, fx_plot, fx_offset = get_fx(adjusted_k_data)
print(fx_type, fx_time)
plot_lines(ax, fx_plot, fx_offset)

# pivot_x_interval, pivot_price_interval = get_pivot(fx_plot, fx_offset, len(fx_offset)-2)
# plot_pivot(ax, pivot_x_interval, pivot_price_interval)
 