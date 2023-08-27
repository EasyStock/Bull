
import os

def GetWorkSpaceRoot():
    defaultRoot = "/Volumes/Data/"
    if os.path.exists(defaultRoot) == False:
        defaultRoot = "/home/jenkins/"
    
    return defaultRoot[:-1]

workSpaceRoot = GetWorkSpaceRoot()



def GetDefaultFont():
    defaultRoot = "/Volumes/Data/"
    defaultFont = "Arial Unicode MS"
    if os.path.exists(defaultRoot) == False:
        defaultFont = "SimHei"
    
    return defaultFont


WorkSpaceFont = GetDefaultFont()