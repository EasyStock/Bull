
import os
import re


def RemoveBigFolder(folder):
    files = os.listdir(folder)
    names = []
    for file in files:
        olderName = os.path.join(folder, file)
        start = file.find("（") + len("（")
        end =file.find("）")
        if start != -1 and end != -1:
            number = file[start:end]
            names.append(number)
            newName = os.path.join(folder,f'''猫和老鼠第{int(number)}集.mp4''')
            print(newName)
            os.rename(olderName, newName)
        else:
            print(olderName)
    
    t = sorted(names, reverse=False)
    print(t)
        # res = re.search('^[\s\S]*(?P<number>[\d]+)[\s\S]*.mp4$',file)
        # if res is not None:
        #     resDict = res.groupdict()
        #     number = resDict["number"]
        #     newName = os.path.join(folder,f'''猫和老鼠第{number}集.mp4''')
        #     os.rename(olderName, newName)
        #     print(newName)


if __name__ == "__main__":
    folder = f'''/Volumes/Share (存储空间2)/影视/猫和老鼠/'''
    RemoveBigFolder(folder=folder)
