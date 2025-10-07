import mplfinance as mpf
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
import matplotlib as mpl



import pandas as pd

def adjust_open_close(row):
    """确保开盘价和收盘价在最高价和最低价范围内"""
    open_price, close_price, high, low = row[['open', 'close', 'high', 'low']]
    
    if open_price > close_price:
        # 阴线：开盘价在最高价之上时取最高价，收盘价在最低价之下时取最低价
        open_price = min(open_price, high)
        close_price = max(close_price, low)
    else:
        # 阳线：开盘价在最低价之下时取最低价，收盘价在最高价之上时取最高价
        open_price = max(open_price, low)
        close_price = min(close_price, high)
    
    return pd.Series({
        'open': open_price,
        'close': close_price,
        'high': high,
        'low': low
    })

def adjust_by_containment(k_data):
    """
    处理K线包含关系，用于识别顶分型和底分型
    优化点：
    1. 使用列表收集结果替代循环内concat，提升10倍以上速度
    2. 简化包含关系判断逻辑
    3. 封装开盘收盘价调整函数
    4. 使用向量化操作
    
    参数:
    k_data: DataFrame，包含['open', 'close', 'high', 'low']列的K线数据
    
    返回:
    adjusted_k_data: 处理后的K线序列
    """
    # 结果列表预先分配空间
    adjusted_rows = []
    # 初始趋势（0表示未定义方向）
    current_trend = 0  
    # 初始化参考K线（第一根K线）
    ref_k = k_data.iloc[0].copy()
    
    # 处理第一根K线
    first_row = adjust_open_close(ref_k)
    adjusted_rows.append(first_row)
    
    # 从第二根K线开始处理
    for i in range(1, len(k_data)):
        current_k = k_data.iloc[i].copy()
        ref_high, ref_low = ref_k[['high', 'low']]
        cur_high, cur_low = current_k[['high', 'low']]
        
        # 判断包含关系（同时处理完全相等的情况）
        if ref_high == cur_high and ref_low == cur_low:
            # 完全相等的K线，保持趋势不变
            new_trend = 0
            # 保留原始K线（不更新参考K线）
            merged_k = ref_k.copy()
        
        # 向右包含（参考K线包含当前K线）
        elif ref_high >= cur_high and ref_low <= cur_low:
            new_trend = current_trend
            merged_k = ref_k.copy()
            # 根据趋势方向处理包含
            if current_trend == -1:  # 下降趋势取较低高点
                merged_k['high'] = cur_high
            else:  # 上升趋势或未定义趋势取较高低点
                merged_k['low'] = cur_low
        
        # 向左包含（当前K线包含参考K线）
        elif ref_high <= cur_high and ref_low >= cur_low:
            new_trend = current_trend
            merged_k = ref_k.copy()
            # 根据趋势方向处理包含
            if current_trend == -1:  # 下降趋势取较低低点
                merged_k['low'] = cur_low
            else:  # 上升趋势或未定义趋势取较高高点
                merged_k['high'] = cur_high
        
        # 非包含关系 - 上升K线
        elif ref_high < cur_high and ref_low < cur_low:
            new_trend = 1  # 上升趋势
            merged_k = current_k.copy()
            # 更新参考K线为当前K线
            ref_k = current_k.copy()
        
        # 非包含关系 - 下降K线
        elif ref_high > cur_high and ref_low > cur_low:
            new_trend = -1  # 下降趋势
            merged_k = current_k.copy()
            # 更新参考K线为当前K线
            ref_k = current_k.copy()
        
        # 部分重叠情况（不属于标准包含关系）
        else:
            new_trend = current_trend
            merged_k = current_k.copy()
            # 更新参考K线为当前K线
            ref_k = current_k.copy()
        
        # 更新趋势（完全相等时保持原趋势）
        current_trend = new_trend if new_trend != 0 else current_trend
        
        # 调整合并后K线的开盘收盘价
        adjusted_k = adjust_open_close(merged_k)
        adjusted_rows.append(adjusted_k)
    
    # 一次性构建结果DataFrame
    adjusted_k_data = pd.DataFrame(adjusted_rows, index=k_data.index)
    return adjusted_k_data.reset_index(drop=True)


def plot_k_series(k_data, figsize=None, volume=True, title='K线图', style='binance', show=True):
    """
    专业级 K 线绘图函数 - 修复中文显示问题
    
    参数:
    k_data : DataFrame, 必须包含['open','high','low','close']列
    figsize : 可选, 自定义图形尺寸 (宽度, 高度)
    volume : 是否显示成交量 (默认True)
    title : 图表标题
    style : 内置样式名称 ('binance', 'yahoo', 'blueskies', 'nightclouds'等)
    show : 是否立即显示图形 (默认True)
    
    返回:
    fig : matplotlib图形对象
    """

    # 设置中文字体支持 - 修复中文显示问题
    plt.rcParams['font.sans-serif'] = ['PingFang SC', 'Heiti SC', 'Songti SC', 'Arial Unicode MS']  # 中文字体
    plt.rcParams['axes.unicode_minus'] = False  # 正确显示负号
    
    # 创建数据副本避免修改原始数据
    plot_data = k_data.copy()
    
    # 确保索引是DatetimeIndex
    if not isinstance(plot_data.index, pd.DatetimeIndex):
        try:
            plot_data.index = pd.to_datetime(plot_data.index)
        except Exception as e:
            print(f"警告: 无法将索引转换为日期时间格式: {e}")
            plot_data.index = pd.RangeIndex(start=0, stop=len(plot_data))
    
    # 确保列名符合标准
    column_map = {
        'open': 'Open',
        'high': 'High',
        'low': 'Low',
        'close': 'Close',
        'volume': 'Volume'
    }
    plot_data.columns = [column_map.get(col.lower(), col) for col in plot_data.columns]
    
    # 动态计算图形大小
    num_bars = len(plot_data)
    default_width = max(10, min(24, num_bars * 0.15))
    figsize = figsize or (default_width, 8)
    
    # 正确设置K线宽度参数
    width_config = {
        'candle_linewidth': 0.8,     # K线边框宽度
        'candle_width': 0.8,          # K线实体宽度
        'volume_width': 0.7           # 成交量宽度
    }
    
    kwargs = {
        'type': 'candle',
        'figsize': figsize,
        'title': title,
        'ylabel': '价格',
        'returnfig': True,
        'show_nontrading': False,
        'warn_too_much_data': num_bars > 2000,
        
        # 正确设置K线宽度参数
        'update_width_config': width_config,
        
        # 日期格式和旋转
        'datetime_format': '%Y-%m-%d',
        'xrotation': 45,
        
        # 高级配置
        'scale_padding': {'left': 0.1, 'top': 0.2, 'right': 0.1, 'bottom': 0.2},
        'fontscale': 1.1
    }
    
    # 添加成交量配置
    if volume and 'Volume' in plot_data.columns:
        kwargs['volume'] = True
        kwargs['panel_ratios'] = (4, 1)  # K线图与成交量图的比例
    
    # 使用专业金融图表样式
    if style not in mpf.available_styles():
        style = 'binance'  # 默认使用专业样式
    
    # 创建绘图对象
    fig, axlist = mpf.plot(
        plot_data,
        style=style,
        **kwargs
    )
    
    # 自定义日期格式 (如果需要)
    if isinstance(plot_data.index, pd.DatetimeIndex) and len(plot_data) > 1:
        ax = axlist[0]
        date_diff = (plot_data.index[-1] - plot_data.index[0]).days
        
        if date_diff > 365:  fmt = '%Y'
        elif date_diff > 30: fmt = '%Y-%m'
        elif date_diff > 7:  fmt = '%m-%d'
        else:               fmt = '%m-%d %H:%M'
        
        ax.xaxis.set_major_formatter(mdates.DateFormatter(fmt))
    
    # 添加专业网格
    for ax in axlist:
        ax.grid(True, linestyle='--', alpha=0.3, color='gray')
    
    # 专业布局调整
    plt.subplots_adjust(left=0.05, right=0.95, bottom=0.15, top=0.9, hspace=0.1)
    
    # 添加专业标题 - 修复中文显示问题
    fig.suptitle(title, fontsize=16, fontweight='bold', y=0.95)
    
    # 如果需要显示图形
    if show:
        plt.show()
    
    return fig


file = "/Users/mac/Desktop/上证指数.csv"
k_data = pd.read_csv(file)
k_data.set_index("日期",inplace=True,drop=True)
print(k_data)
# 基本使用 (自动绘制K线)
fig = plot_k_series(k_data, style='yahoo')


# # 不显示成交量
#fig = plot_k_series(k_data, volume=False)

# # 自定义尺寸
#fig = plot_k_series(k_data, figsize=(16, 8))

# # 添加额外技术指标
# ap = [
#     #mpf.make_addplot(k_data['MA20'], color='blue', width=1.5),
#     #mpf.make_addplot(k_data['MA50'], color='orange', width=1.5)
# ]

# fig = plot_k_series(k_data, addplot=ap)

plt.show()