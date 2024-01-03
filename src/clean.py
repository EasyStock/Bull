import os
import json
import shutil
from ColoredLog import StartToInitLogger
import logging
logger = logging.getLogger()

# cleanDir = [".git",".DS_Store",".gitignore"]
cleanDir = ["__pycache__",".DS_Store"]

def ToSizeInfo(size):
    if size < 1024:
        return f'''{float(size)} bytes'''
    elif size < 1024*1024:
        return f'''{float(size)/1024:.2f} KB'''
    elif size < 1024*1024*1024:
        return f'''{float(size)/1024/1024:.2f} MB'''
    else :
        return f'''{float(size)/1024/1024/1024:.2f} GB'''
    
    return size

def getSize(path):
    sizeInfo = {}
    totalSize = 0
    if os.path.isfile(path):
        s = os.path.getsize(path)
        sizeInfo[path] = (s)
        totalSize = s
        return (totalSize,sizeInfo)
    
    elif os.path.isdir(path):
        files = os.listdir(path)
        for file in files:
            fullPath = os.path.join(path,file)
            if os.path.isfile(fullPath):
                s = os.path.getsize(fullPath)
                sizeInfo[fullPath] = (s)
                totalSize = totalSize + s
            elif os.path.isdir(fullPath):
                t = getSize(fullPath)
                sizeInfo.update(t[1])
                totalSize = totalSize + t[0]
    sizeInfo[path] = (totalSize)
    return (totalSize,sizeInfo)


def ScanFolder(folder,printFullPath = False,deleteUseless = False):
    for root,dirs,files in os.walk(folder):
        
        for file in files:
            path = os.path.join(root,file)
            if file in cleanDir:
                if deleteUseless:
                    os.remove(path)
                    msg = f'''remove file:{path}'''
                    logger.info(msg)
                print(path)
            if printFullPath:
                print(path)
        for dir in dirs:
            path = os.path.join(root,dir)
            if dir in cleanDir:
                if deleteUseless:
                    shutil.rmtree(path)
                    msg = f'''remove folder:{path}'''
                    logger.info(msg)

                print(path)
            if printFullPath:
                print(path)

    
def RemoveUselessFiles(folder):
    logging.info(f'''start to remove useless folder: {folder}''')
    size  = getSize(folder)
    # ScanFolder(folder,False,True)
    # ScanFolder(folder,False,False)
    folerInfo = size[1]
    for info in folerInfo:
        last = info[info.rfind("/")+1:]
        if last in cleanDir and os.path.exists(info):
            if os.path.isdir(info):
                try:
                    msg = f'''remove FOLDER:{info}'''
                    logger.info(msg)
                    shutil.rmtree(info)
                except:
                    logger.warning(info)
                    #os.rmdir(info)
            elif os.path.isfile(info):
                msg = f'''remove FILE:{info}'''
                try:
                    os.remove(info)
                    logger.info(msg)
                except:
                    logger.error(msg)

    size1  = getSize(folder)
    msg = f'''{folder} before: {ToSizeInfo(size[0])}, after: {ToSizeInfo(size1[0])} total remove : {ToSizeInfo(size[0] - size1[0])}'''
    logging.error(msg)
    t = size1[1]
    s = sorted(t.items(),key = lambda x:x[1],reverse = True)
    u = s
    if len(s) >10:
        u = s[:10]
    for k in u:
        msg = f'''{k[0]} : {ToSizeInfo(k[1])}'''
        logging.info(msg)

    #logging.info(json.dumps(size[1],indent=3))

def CalcSize(folder):
    size  = getSize(folder)
    t = size[1]
    s = sorted(t.items(),key = lambda x:x[1],reverse = True)
    for k in s:
        msg = f'''{k[0]} : {ToSizeInfo(k[1])}'''
        logging.info(msg)

def RemoveBigFolder(folder):
    files = os.listdir(folder)
    for file in files:
        fullPath = os.path.join(folder,file)
        if fullPath.find(".") != -1:
            continue
        if os.path.isdir(fullPath):
            RemoveUselessFiles(fullPath)

if __name__ == "__main__":
    folder = '/Volumes/Data/Code/EasyStock/Bull/src'
    StartToInitLogger("CalcSize")
    #RemoveBigFolder(folder)
    RemoveUselessFiles(folder)