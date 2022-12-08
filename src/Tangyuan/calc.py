import random
import pdfkit


def RandomPlus(N,min1,max1,min2,max2):
    result = []
    for _ in range(N):
        first = random.randint(min1,max1)
        second  = random.randint(min2,max2)
        result.append((first,"+",second))
    
    return result


def RandomSubtraction(N,min1,max1):
    result = []
    for _ in range(N):
        first = random.randint(min1,max1)
        second  = random.randint(min1,max1)
        if first > second:
            result.append((first,"-",second))
        else:
            result.append((second,"-",first))
    
    return result




def format():
    result = []
    #plusCount = random.randint(0,129)
    totalCount = 120
    plusCount = int(totalCount*0.6)
    r1 = RandomPlus(plusCount,6,50,6,50)
    r2 = RandomSubtraction(totalCount-plusCount,6,50)
    result.extend(r1)
    result.extend(r2)
    random.shuffle(result)
    #print(result)
    title = f'''{"":10}{"姓名:____________    日期:___________    做题时间:__________"}\n\n'''
    resultStr = ""
    for index,r in enumerate(result):
        #print(index,r)
        if (index) % 30 == 0  and index != 0:
            resultStr = resultStr + title
            
        msg = f'''{"":3}{r[0]:<5}{r[1]}{r[2]:>5} = {"":10}'''
        resultStr = resultStr + msg
        if (index+1) % 3 == 0:
            resultStr = resultStr + '\n\n'
            
    resultStr = resultStr + title
    print(resultStr)
    with open("/tmp/aa.txt","w+") as f:
        f.write(resultStr)




if __name__ == "__main__":
    format()