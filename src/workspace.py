
import os

def GetWorkSpaceRoot():
    defaultRoot = "workSpaceRoot"
    if os.path.exists(defaultRoot) == False:
        defaultRoot = "/home/jenkins"
    
    return defaultRoot

workSpaceRoot = GetWorkSpaceRoot()