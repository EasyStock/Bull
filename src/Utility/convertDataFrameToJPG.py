
from workspace import workSpaceRoot,WorkSpaceFont
import pandas as pd
import logging

def ConvertDataFrameToJPG(df,fullPath):
    if df.empty:
        return
    
    from pandas.plotting import table
    import matplotlib.pyplot as plt
    plt.rcParams["font.sans-serif"] = [WorkSpaceFont]#显示中文字体
    high = int(0.174 * df.shape[0]+0.5)+1
    fig = plt.figure(figsize=(3, high), dpi=200)#dpi表示清晰度
    ax = fig.add_subplot(111, frame_on=False) 
    ax.xaxis.set_visible(False)  # hide the x axis
    ax.yaxis.set_visible(False)  # hide the y axis
    table(ax, df, loc='center')  # 将df换成需要保存的dataframe即可
    plt.savefig(fullPath)
    plt.close()

def DataFrameToJPG(df,columns,rootPath, fileName):
    if df.empty:
        return
    size = df.shape[0]
    step = 80
    if size > step:
        for index in range(0,size,step):
            tmp = df.iloc[index:,]
            if index + step <= size:
                tmp = df.iloc[index:index+step,]
            fullPath = f"{rootPath}{fileName}_{int(index/step+1)}.jpg"
            logging.info(fullPath)
            jpgDataFrame = pd.DataFrame(tmp,columns=columns)
            ConvertDataFrameToJPG(jpgDataFrame,fullPath)
    else:
        fullPath = f"{rootPath}{fileName}.jpg"
        logging.info(fullPath)
        jpgDataFrame = pd.DataFrame(df,columns=columns)
        ConvertDataFrameToJPG(jpgDataFrame,fullPath)