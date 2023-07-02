
import os

def GetWorkSpaceRoot():
    defaultRoot = "/Volumes/Data/"
    if os.path.exists(defaultRoot) == False:
        defaultRoot = "/home/jenkins/"
    
    return defaultRoot[:-1]

workSpaceRoot = GetWorkSpaceRoot()