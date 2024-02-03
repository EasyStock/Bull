
import os

def GetWorkSpaceRoot():
    defaultRoot = "/Volumes/Data/"
    if os.path.exists(defaultRoot) == False:
        defaultRoot = "/home/jenkins/"
    
    return defaultRoot[:-1]

def GetFuPanRoot(date):
    year, month, _ = date.split('-')
    folder = f'''{year}年{month}月/复盘/'''
    root = GetWorkSpaceRoot()
    stockFolder = os.path.join(root,folder)
    if os.path.exists(stockFolder) == False:
        os.makedirs(stockFolder)
    return stockFolder

def GetStockFolder(date):
    folder = f'''/股票/{date}'''
    root = GetFuPanRoot(date)
    stockFolder = os.path.join(root,folder)
    if os.path.exists(stockFolder) == False:
        os.makedirs(stockFolder)
    return stockFolder

def GetZhuanZaiFolder(date):
    folder = f'''/可转债/{date}'''
    root = GetFuPanRoot(date)
    stockFolder = os.path.join(root,folder)
    if os.path.exists(stockFolder) == False:
        os.makedirs(stockFolder)
    return stockFolder

def GetDefaultFont():
    defaultRoot = "/Volumes/Data/"
    defaultFont = "Arial Unicode MS"
    if os.path.exists(defaultRoot) == False:
        defaultFont = "SimHei"
    
    return defaultFont


WorkSpaceFont = GetDefaultFont()
workSpaceRoot = GetWorkSpaceRoot()