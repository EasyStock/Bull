from kaipanla.kaipanlaAPI import CkaiPanLaApi
import datetime
import json
import pandas as pd


class CKaiPanLaDataMgr(object):
    def __init__(self) -> None:
        pass

    def RequestDataToday(self,params):
        url = params["urlOfToday"].strip()
        queryString = params["queryStringOfToday"].strip()
        host  = params["hostOfToday"].strip()
        api = CkaiPanLaApi(url,queryString,host)
        data = api.RequstData()
        return data


    def RequestHistoryData(self,date,params):
        url = params["urlOfHistory"].strip()
        queryString = params["queryStringOfHistory"].strip().format(date)
        host  = params["hostOfHistory"].strip()
        api = CkaiPanLaApi(url,queryString,host)
        data = api.RequstData()
        return data


    def RequestData(self,date,params:dict):
        today = datetime.date.today()
        data = None
        if str(today) == str(date):
            data = self.RequestDataToday(params)
        else:
            data = self.RequestHistoryData(date,params)

        return data

#=========================================================================================
def DataFrameToSqls_INSERT_OR_IGNORE(datas,tableName):
    sqls = []
    for _, row in datas.iterrows():
        index_str = '''`,`'''.join(row.index)
        value_str = '''","'''.join(str(x) for x in row.values)
        sql = '''INSERT IGNORE INTO {0} (`{1}`) VALUES ("{2}");'''.format(tableName,index_str,value_str)
        sqls.append(sql)
    return sqls

def _timestampToStr(t):
    d = datetime.datetime.fromtimestamp(float(t))
    s = d.strftime("%H:%M:%S")
    return s

def _formatVolumn(volumn,delta = 1.0):
    newVolumn = float(volumn) * delta
    t = newVolumn / 10000.0
    ret = f'''{t:.0f}'''
    return ret

def RequestVolumnDataByDates(dates,dbConnection):  #大盘成交量
    KaiPanLaVolumnParam = {
    "urlOfToday":"https://apphq.longhuvip.com/w1/api/index.php",
    "queryStringOfToday":"PhoneOSNew=2&Type=0&VerSion=5.7.0.15&a=MarketCapacity&apiv=w32&c=HomeDingPan",
    "hostOfToday":"apphq.longhuvip.com",
    #====================
    "urlOfHistory":"https://apphis.longhuvip.com/w1/api/index.php",
    "queryStringOfHistory" :"Date={0}&PhoneOSNew=2&Type=0&VerSion=5.9.0.3&a=MarketCapacity&apiv=w32&c=HisHomeDingPan",
    "hostOfHistory":"apphis.longhuvip.com"
    }

    dataMgr = CKaiPanLaDataMgr()
    for date in dates:
        print(f"=======start to request 两市成交量 data of: {date}=================")
        result = dataMgr.RequestData(date,KaiPanLaVolumnParam)
        if result is not None:
            js =json.loads(result)
            last = float(js['info']['last'])
            s_zrcs = float(js['info']['s_zrcs'])
            trends = str(js['info']['trends']).replace("'","\"")
            delta = last - s_zrcs
            ratio = delta*1.0/s_zrcs*100
            sql = f'''INSERT IGNORE INTO `stock`.`kaipanla_volumn` (`date`, `volumn`, `s_zrcs`, `delta`, `ratio`, `trends`) VALUES ('{date}', '{last/10000:.0f}亿', '{s_zrcs/10000:.0f}亿', '{delta/10000:.0f}亿', '{ratio:.2f}%', '{trends}');'''
            dbConnection.Execute(sql)
        else:
            print(f'''{date} data is None''')


def RequestZhangTingDataByDates(dates,dbConnection): # 涨停数据
    KaiPanLaZhangTingListParam = {
        "urlOfToday":"https://apphq.longhuvip.com/w1/api/index.php",
        "queryStringOfToday":"Filter=0&FilterGem=0&FilterMotherboard=0&FilterTIB=0&Index=0&Is_st=1&Order=1&PhoneOSNew=2&PidType=1&Type=9&VerSion=5.7.0.15&a=DaBanList&apiv=w32&c=HomeDingPan&st=1000",
        "hostOfToday":"apphq.longhuvip.com",
        #====================
        "urlOfHistory":"https://apphis.longhuvip.com/w1/api/index.php",
        "queryStringOfHistory" :"Day={0}&Filter=0&FilterGem=0&FilterMotherboard=0&FilterTIB=0&Index=0&Is_st=1&Order=1&PhoneOSNew=2&PidType=1&Type=6&VerSion=5.7.0.15&a=HisDaBanList&apiv=w32&c=HisHomeDingPan&st=1000",
        "hostOfHistory":"apphis.longhuvip.com"
        }
    # pd.set_option('display.unicode.ambiguous_as_wide',True)
    # pd.set_option('display.unicode.east_asian_width',True)
    # pd.set_option('display.width',180)
    dataMgr = CKaiPanLaDataMgr()
    for date in dates:
        print(f"=======start to request 涨停 data of: {date}=================")
        result = dataMgr.RequestData(date,KaiPanLaZhangTingListParam)
        if result is not None:
            js =json.loads(result)
            l = js['list']
            df = pd.DataFrame(l)
            if df.empty:
                continue

            res = pd.DataFrame()
            
            res["stockID"] = df[0]
            res["stockName"] = df[1]
            res["firstTime"] = df[6]
            res["status"] = df[9]
            res["reason"] = df[16]
            res["bankuai"] = df[11]
            res["lastTime"] = df[25]
            res["fengdanMax"] = df[23]
            res["fengdan"] = df[8]
            res["jinge"] = df[12]
            res["volumn"] = df[13]
            res["huanshou"] = df[14]
            res["liutong"] = df[15]
            res["date"] = str(date)
            res['firstTime'] = res.apply(lambda row: _timestampToStr(row['firstTime']), axis=1)
            res['lastTime'] = res.apply(lambda row: _timestampToStr(row['lastTime']), axis=1)
            res['fengdanMax'] = res.apply(lambda row: _formatVolumn(row['fengdanMax']), axis=1)
            res['fengdan'] = res.apply(lambda row: _formatVolumn(row['fengdan']), axis=1)
            res['jinge'] = res.apply(lambda row: _formatVolumn(row['jinge']), axis=1)
            res['volumn'] = res.apply(lambda row: _formatVolumn(row['volumn']), axis=1)
            res['liutong'] = res.apply(lambda row: _formatVolumn(row['liutong']), axis=1)
            sqls = DataFrameToSqls_INSERT_OR_IGNORE(res,"`stock`.`kaipanla_zhangting`")
            for sql in sqls:
                #print(sql)
                dbConnection.Execute(sql)
        else:
            print(f'''{date} data is None''')

def RequestZhaBanDataByDates(dates,dbConnection): #炸板数据
    KaiPanLaZhaBanListParam = {
        "urlOfToday":"https://apphq.longhuvip.com/w1/api/index.php",
        "queryStringOfToday":"Filter=0&FilterGem=0&FilterMotherboard=0&FilterTIB=0&Index=0&Is_st=1&Order=0&PhoneOSNew=2&PidType=2&Type=4&VerSion=5.7.0.15&a=DaBanList&apiv=w32&c=HomeDingPan&st=1000",
        "hostOfToday":"apphq.longhuvip.com",
        #====================
        "urlOfHistory":"https://apphis.longhuvip.com/w1/api/index.php",
        "queryStringOfHistory" :"Day={0}&Filter=0&FilterGem=0&FilterMotherboard=0&FilterTIB=0&Index=0&Is_st=1&Order=0&PhoneOSNew=2&PidType=2&Type=4&VerSion=5.7.0.15&a=HisDaBanList&apiv=w32&c=HisHomeDingPan&st=1000",
        "hostOfHistory":"apphis.longhuvip.com"
        }
    # pd.set_option('display.unicode.ambiguous_as_wide',True)
    # pd.set_option('display.unicode.east_asian_width',True)
    # pd.set_option('display.width',180)
    dataMgr = CKaiPanLaDataMgr()
    for date in dates:
        print(f"=======start to request 炸板 data of: {date}=================")
        result = dataMgr.RequestData(date,KaiPanLaZhaBanListParam)
        if result is not None:
            js =json.loads(result)
            l = js['list']
            df = pd.DataFrame(l)
            if df.empty:
                continue

            res = pd.DataFrame()
            
            res["stockID"] = df[0]
            res["stockName"] = df[1]
            res["zhangfu"] = df[4]

            res["time"] = df[6]
            res["timezhaban"] = df[7]

            res["bankuai"] = df[11]

            res["jinge"] = df[12]
            res["volumn"] = df[13]
            res["huanshou"] = df[14]
            res["liutong"] = df[15]
            res["date"] = str(date)

            res['time'] = res.apply(lambda row: _timestampToStr(row['time']), axis=1)
            res['timezhaban'] = res.apply(lambda row: _timestampToStr(row['timezhaban']), axis=1)

            res['jinge'] = res.apply(lambda row: _formatVolumn(row['jinge']), axis=1)
            res['volumn'] = res.apply(lambda row: _formatVolumn(row['volumn']), axis=1)
            res['liutong'] = res.apply(lambda row: _formatVolumn(row['liutong']), axis=1)
            sqls = DataFrameToSqls_INSERT_OR_IGNORE(res,"`stock`.`kaipanla_zhaban`")
            for sql in sqls:
                #print(sql)
                dbConnection.Execute(sql)
        else:
            print(f'''{date} data is None''')


def RequestDieTingDataByDates(dates,dbConnection): #跌停数据
    KaiPanLaDieTingListParam = {
        "urlOfToday":"https://apphq.longhuvip.com/w1/api/index.php",
        "queryStringOfToday":"Filter=0&FilterGem=0&FilterMotherboard=0&FilterTIB=0&Index=0&Is_st=1&Order=0&PhoneOSNew=2&PidType=3&Type=4&VerSion=5.7.0.15&a=DaBanList&apiv=w32&c=HomeDingPan&st=1000",
        "hostOfToday":"apphq.longhuvip.com",
        #====================
        "urlOfHistory":"https://apphis.longhuvip.com/w1/api/index.php",
        "queryStringOfHistory" :"Day={0}&Filter=0&FilterGem=0&FilterMotherboard=0&FilterTIB=0&Index=0&Is_st=1&Order=0&PhoneOSNew=2&PidType=3&Type=4&VerSion=5.7.0.15&a=HisDaBanList&apiv=w32&c=HisHomeDingPan&st=1000",
        "hostOfHistory":"apphis.longhuvip.com"
        }
    # pd.set_option('display.unicode.ambiguous_as_wide',True)
    # pd.set_option('display.unicode.east_asian_width',True)
    # pd.set_option('display.width',180)
    dataMgr = CKaiPanLaDataMgr()
    for date in dates:
        print(f"=======start to request 跌停 data of: {date}=================")
        result = dataMgr.RequestData(date,KaiPanLaDieTingListParam)
        if result is not None:
            js =json.loads(result)
            l = js['list']
            df = pd.DataFrame(l)
            
            if df.empty:
                continue
            res = pd.DataFrame()
            
            res["stockID"] = df[0]
            res["stockName"] = df[1]

            res["time"] = df[6]
            res["fengdan"] = df[8]

            res["bankuai"] = df[11]

            res["jinge"] = df[12]
            res["volumn"] = df[13]
            res["huanshou"] = df[14]
            res["liutong"] = df[15]
            res["date"] = str(date)

            res['time'] = res.apply(lambda row: _timestampToStr(row['time']), axis=1)

            res['fengdan'] = res.apply(lambda row: _formatVolumn(row['fengdan']), axis=1)
            res['jinge'] = res.apply(lambda row: _formatVolumn(row['jinge']), axis=1)
            res['volumn'] = res.apply(lambda row: _formatVolumn(row['volumn']), axis=1)
            res['liutong'] = res.apply(lambda row: _formatVolumn(row['liutong']), axis=1)
            #print(res)
            sqls = DataFrameToSqls_INSERT_OR_IGNORE(res,"`stock`.`kaipanla_dieting`")
            for sql in sqls:
                #print(sql)
                dbConnection.Execute(sql)
        else:
            print(f'''{date} data is None''')




def RequestZhiRanZhangTingDataByDates(dates,dbConnection): # 自然涨停数据
    KaiPanLaZhiranZhangTingListParam = {
        "urlOfToday":"https://apphq.longhuvip.com/w1/api/index.php",
        "queryStringOfToday":"Filter=0&FilterGem=0&FilterMotherboard=0&FilterTIB=0&Index=0&Is_st=1&Order=0&PhoneOSNew=2&PidType=4&Type=6&VerSion=5.7.0.15&a=DaBanList&apiv=w32&c=HomeDingPan&st=1000",
        "hostOfToday":"apphq.longhuvip.com",
        #====================
        "urlOfHistory":"https://apphis.longhuvip.com/w1/api/index.php",
        "queryStringOfHistory" :"Day={0}&Filter=0&FilterGem=0&FilterMotherboard=0&FilterTIB=0&Index=0&Is_st=1&Order=0&PhoneOSNew=2&PidType=4&Type=6&VerSion=5.7.0.15&a=HisDaBanList&apiv=w32&c=HisHomeDingPan&st=1000",
        "hostOfHistory":"apphis.longhuvip.com"
        }
    # pd.set_option('display.unicode.ambiguous_as_wide',True)
    # pd.set_option('display.unicode.east_asian_width',True)
    # pd.set_option('display.width',180)
    dataMgr = CKaiPanLaDataMgr()
    for date in dates:
        print(f"=======start to request 自然涨停 data of: {date}=================")
        result = dataMgr.RequestData(date,KaiPanLaZhiranZhangTingListParam)
        if result is not None:
            js =json.loads(result)
            l = js['list']
            df = pd.DataFrame(l)
            if df.empty:
                continue
            #print(df)
            res = pd.DataFrame()
            
            res["stockID"] = df[0]
            res["stockName"] = df[1]
            res["time"] = df[6]
            res["status"] = df[9]
            res["reason"] = df[16]
            res["bankuai"] = df[11]

            res["fengdanMax"] = df[23]
            res["fengdan"] = df[8]
            res["jinge"] = df[12]
            res["volumn"] = df[13]
            res["huanshou"] = df[14]
            res["liutong"] = df[15]
            res["date"] = str(date)

            res['time'] = res.apply(lambda row: _timestampToStr(row['time']), axis=1)
            res['fengdanMax'] = res.apply(lambda row: _formatVolumn(row['fengdanMax']), axis=1)
            res['fengdan'] = res.apply(lambda row: _formatVolumn(row['fengdan']), axis=1)
            res['jinge'] = res.apply(lambda row: _formatVolumn(row['jinge']), axis=1)
            res['volumn'] = res.apply(lambda row: _formatVolumn(row['volumn']), axis=1)
            res['liutong'] = res.apply(lambda row: _formatVolumn(row['liutong']), axis=1)
            sqls = DataFrameToSqls_INSERT_OR_IGNORE(res,"`stock`.`kaipanla_ziranzhangting`")
            for sql in sqls:
                #print(sql)
                dbConnection.Execute(sql)
        else:
            print(f'''{date} data is None''')




# class CKaiPanLaVolumnMgr(object):
#     def __init__(self,dbConnection) -> None:
#         self.dbConnection = dbConnection

#     def RequestDataToday(self):
#         url = "https://apphq.longhuvip.com/w1/api/index.php"
#         queryString = "PhoneOSNew=2&Type=0&VerSion=5.7.0.15&a=MarketCapacity&apiv=w32&c=HomeDingPan"
#         host  = 'apphq.longhuvip.com'
#         api = CkaiPanLaApi(url,queryString,host)
#         data = api.RequstData()
#         return data


#     def RequestHistoryData(self,date):
#         url = "https://apphis.longhuvip.com/w1/api/index.php"
#         queryString = f'''Date={date}&PhoneOSNew=2&Type=0&VerSion=5.9.0.3&a=MarketCapacity&apiv=w32&c=HisHomeDingPan'''
#         host  = 'apphis.longhuvip.com'
#         api = CkaiPanLaApi(url,queryString,host)
#         data = api.RequstData()
#         return data


#     def RequestData(self,date):
#         today = datetime.date.today()
#         data = None
#         if str(today) == str(date):
#             data = self.RequestDataToday()
#         else:
#             data = self.RequestHistoryData(date)

#         print(f"=======start to request {date}=================")
#         js =json.loads(data)
#         last = js['info']['last']
#         s_zrcs = js['info']['s_zrcs']
#         trends = str(js['info']['trends']).replace("'","\"")
#         sql = f'''INSERT IGNORE INTO `stock`.`kaipanla_volumn` (`date`, `volumn`, `s_zrcs`, `trends`) VALUES ('{date}', '{last}', '{s_zrcs}', '{trends}');'''
#         #print(sql)
#         self.dbConnection.Execute(sql)

#     def RequestDatabyDates(self,dates):
#         for date in dates:
#             self.RequestData(date)

#=========================================================================================#