
import re 
import os

def MergeLeetCode(en = True):
    folder = "/Users/mac/Downloads/leetcode-main/solution"
    results = []
    for root,dirs,files in os.walk(folder):
        for file in files:
            path = os.path.join(root,file)
            #print(path)
            if re.match(".*\d{4}-\d{4}.*",path) == None:
                continue
            if en and file == "README_EN.md":    
                results.append(path)
            elif en == False and file == "README.md":
                results.append(path)
    
    results.sort(reverse=False)
    #print(results)
    with open("/tmp/merge.md","w+") as my_file:
        for f in results:
            r = ReadLeetCodeMarkDown(f,en)
            my_file.write(r)



def ReadLeetCodeMarkDown(filename,en = True):
    with open(filename) as my_file:
        contexts = my_file.read()
        if en:
            new = re.sub("\[中文文档\].*\n","",contexts)
        else:
            new = re.sub("\[English Version\].*\n","",contexts)

        # new1 = re.sub("## 解法[\s\S]*$","",new)
        # r = re.sub("## Solutions[\s\S]*$","",new1)

        new1 = re.sub("### \*\*\.\.\.\*\*[\s\S]*<","<",new)
        splits = new1.split("### **")
        results = []
        t1 = ["Python3\*\*","Java\*\*","Go\*\*","JavaScript\*\*","C#\*\*","TypeScript\*\*","Rust\*\*","PHP\*\*","Swift\*\*","Nim\*\*","C\*\*","Ruby\*\*"]
        patten = "|".join(t1)
        for sp in splits:
            if not re.match(patten,sp):
                if re.match("C\+\+\*\*",sp):
                    results.append("### **"+sp)
                else:
                    results.append(sp)
        

        r = "".join(results)
        return r


if __name__ == "__main__":
    # filename="/Users/mac/Downloads/leetcode-main/solution/0000-0099/0070.Climbing Stairs/README_EN_1.md"
    # ReadLeetCodeMarkDown(filename)
    # print(filename)
    MergeLeetCode(False)
