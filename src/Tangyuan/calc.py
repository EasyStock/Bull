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
    plusCount = int(129*0.6)
    r1 = RandomPlus(plusCount,6,20,6,30)
    r2 = RandomSubtraction(129-plusCount,6,30)
    result.extend(r1)
    result.extend(r2)
    random.shuffle(result)
    #print(result)
    s = f'''{"":20}{"姓名:            时间:       "}\n'''
    for index,r in enumerate(result):
        #print(index,r)
        msg = f'''{"":3}{r[0]:<5}{r[1]}{r[2]:>5} = {"":10}'''
        s = s+ msg
        if (index+1) % 3 == 0:
            s = s + '\n\n'

    print(s)
    with open("/tmp/aa.txt","w+") as f:
        f.write(s)




if __name__ == "__main__":
    format()