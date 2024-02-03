
import os

def GetWorkSpaceRoot():
    defaultRoot = "/Volumes/Data/"
    if os.path.exists(defaultRoot) == False:
        defaultRoot = "/home/jenkins/"
    
    return defaultRoot[:-1]

def GetFuPanRoot(date):
    year, month, _ = date.split('-')
    folder = f'''复盘/{year}年{month}月/{date}/'''
    root = GetWorkSpaceRoot()
    stockFolder = os.path.join(root,folder)
    if os.path.exists(stockFolder) == False:
        os.makedirs(stockFolder)
    return stockFolder

def GetStockFolder(date):
    root = GetFuPanRoot(date)
    stockFolder = f'''{root}/股票/'''
    if os.path.exists(stockFolder) == False:
        os.makedirs(stockFolder)
    return stockFolder

def GetZhuanZaiFolder(date):
    root = GetFuPanRoot(date)
    stockFolder = f'''{root}/可转债/'''
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