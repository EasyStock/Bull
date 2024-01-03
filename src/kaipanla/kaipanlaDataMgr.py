from kaipanla.kaipanlaAPI import CkaiPanLaApi
import datetime
import json
import pandas as pd
import pytz
import re

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
        today = datetime.datetime.now(pytz.timezone('Asia/Shanghai')).date()
        data = None
        if str(today) == str(date):
            data = self.RequestDataToday(params)
            return data
        else:
            data = self.RequestHistoryData(date,params)

        return data

class CKaiPanLaMultiPageDataMgr(object):
    def __init__(self,st = 50) -> None:
        self.index  = 0
        self.st = st
        self.df = None
        self.page = 1
        self.datas = []
    
    def _formatURL(self,params):
        queryString1 = params["queryStringOfToday"].strip()
        newIndex = f'''&Index={self.index}&'''
        queryString1 = re.sub("&Index=\d{1,}&",newIndex,queryString1)
        newSt1 = f'''&st={self.st}'''
        queryString1 = re.sub("&st=\d{1,}",newSt1,queryString1)
        params["queryStringOfToday"] = queryString1

        queryString2 = params["queryStringOfHistory"].strip()
        newSt2 = f'''&st={self.st}'''
        queryString2 = re.sub("&Index=\d{1,}&",newIndex,queryString2)
        queryString2 = re.sub("&st=\d{1,}",newSt2,queryString2)

        params["queryStringOfHistory"] = queryString2
        return params

    def RequestData(self,date,params:dict):
        #print(f"开始获取第{self.page}页数据,每页{self.st}条, 开始的索引是{self.index}")
        dataMgr = CKaiPanLaDataMgr()
        newParam = self._formatURL(params)
        #print(newParam)
        result = dataMgr.RequestData(date,newParam)
        if result is not None:
            js =json.loads(result)
            l = js['list']
            [self.datas.append(x) for x in l if x not in self.datas]
            if self.page == 1:
                if len(l) < self.st:
                    self.df = pd.DataFrame(self.datas)
                    print(f"总共获取了:{len(self.datas)}条数据")
                    return self.df
                else:
                    self.index = self.page* self.st -1
                    self.page = self.page + 1
                    self.RequestData(date,newParam)
            else:
                if len(l) < self.st:
                    #最后一页
                    self.df = pd.DataFrame(self.datas)
                    return self.df
                else:
                    self.index = self.page* self.st -1
                    self.page = self.page + 1
                    self.RequestData(date,newParam)
        else:
            self.df = pd.DataFrame(l)

        print(f"总共获取了:{len(self.datas)}条数据")
        return self.df
    
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

def RequestZhangDieTingJiashu(dates,dbConnection):  #涨跌停家数
    KaiPanLaVolumnParam = {
    "urlOfToday":"https://apphq.longhuvip.com/w1/api/index.php",
    "queryStringOfToday":"PhoneOSNew=2&VerSion=5.12.0.1&a=ZhangFuDetail&apiv=w34&c=HomeDingPan",
    "hostOfToday":"apphq.longhuvip.com",
    #====================
    "urlOfHistory":"https://apphis.longhuvip.com/w1/api/index.php",
    "queryStringOfHistory" :"Day={0}&PhoneOSNew=2&VerSion=5.12.0.1&a=HisZhangFuDetail&apiv=w34&c=HisHomeDingPan",
    "hostOfHistory":"apphis.longhuvip.com"
    }
    all = {}
    dataMgr = CKaiPanLaDataMgr()
    for date in dates:
        print(f"=======start to request 涨跌停家数 data of: {date}=================")
        result = dataMgr.RequestData(date,KaiPanLaVolumnParam)
        if result is not None:
            js =json.loads(result)
            dieTing = float(js['info']['SJDT'])
            zhangTing = float(js['info']['SJZT'])
            all[date] = (zhangTing,dieTing)
        else:
            print(f'''{date} data is None''')
        
    return all

def RequestVolumnDataByDates(dates,dbConnection):  #大盘成交量
    KaiPanLaVolumnParam = {
    "urlOfToday":"https://apphq.longhuvip.com/w1/api/index.php",
    "queryStringOfToday":"PhoneOSNew=2&Type=0&VerSion=5.11.0.3&a=MarketCapacity&apiv=w33&c=HomeDingPan",
    "hostOfToday":"apphq.longhuvip.com",
    #====================
    "urlOfHistory":"https://apphis.longhuvip.com/w1/api/index.php",
    "queryStringOfHistory" :"Date={0}&PhoneOSNew=2&Type=0&VerSion=5.12.0.1&a=MarketCapacity&apiv=w34&c=HisHomeDingPan",
    "hostOfHistory":"apphis.longhuvip.com"
    }

    res = {}
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
            res[date] = (f"{last/10000:.0f}亿",f"{delta/10000:.0f}亿",f"{ratio:.2f}%")
        else:
            print(f'''{date} data is None''')
    return res

def RequestZhangTingDataByDates(dates,dbConnection): # 涨停数据
    KaiPanLaZhangTingListParam = {
        "urlOfToday":"https://apphq.longhuvip.com/w1/api/index.php",
        "queryStringOfToday":"Filter=0&FilterGem=0&FilterMotherboard=0&FilterTIB=0&Index=0&Is_st=1&Order=0&PhoneOSNew=2&PidType=1&Type=4&VerSion=5.11.0.3&a=DaBanList&apiv=w33&c=HomeDingPan&st=100",
        "hostOfToday":"apphq.longhuvip.com",
        #====================
        "urlOfHistory":"https://apphis.longhuvip.com/w1/api/index.php",
        "queryStringOfHistory" :"Day={0}&Filter=0&FilterGem=0&FilterMotherboard=0&FilterTIB=0&Index=0&Is_st=1&Order=1&PhoneOSNew=2&PidType=1&Type=6&VerSion=5.11.0.3&a=HisDaBanList&apiv=w33&c=HisHomeDingPan&st=1000",
        "hostOfHistory":"apphis.longhuvip.com"
        }
    
    # pd.set_option('display.unicode.ambiguous_as_wide',True)
    # pd.set_option('display.unicode.east_asian_width',True)
    # pd.set_option('display.width',180)
    for date in dates:
        print(f"=======start to request 涨停 data of: {date}=================")
        dataMgr = CKaiPanLaMultiPageDataMgr()
        df = dataMgr.RequestData(date,KaiPanLaZhangTingListParam)
        if df is not None and not df.empty:
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
                             
        "queryStringOfToday":"Filter=0&FilterGem=0&FilterMotherboard=0&FilterTIB=0&Index=0&Is_st=1&Order=0&PhoneOSNew=2&PidType=2&Type=4&VerSion=5.11.0.3&a=DaBanList&apiv=w33&c=HomeDingPan&st=100",
        "hostOfToday":"apphq.longhuvip.com",
        #====================
        "urlOfHistory":"https://apphis.longhuvip.com/w1/api/index.php",
        "queryStringOfHistory" :"Day={0}&Filter=0&FilterGem=0&FilterMotherboard=0&FilterTIB=0&Index=0&Is_st=1&Order=0&PhoneOSNew=2&PidType=2&Type=4&VerSion=5.11.0.3&a=HisDaBanList&apiv=w33&c=HisHomeDingPan&st=100",
        "hostOfHistory":"apphis.longhuvip.com"
        }
    ret = {}
    for date in dates:
        print(f"=======start to request 炸板 data of: {date}=================")
        dataMgr = CKaiPanLaMultiPageDataMgr()
        df = dataMgr.RequestData(date,KaiPanLaZhaBanListParam)
        if df is not None and not df.empty:
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
            ret[date] = res.shape[0]
            sqls = DataFrameToSqls_INSERT_OR_IGNORE(res,"`stock`.`kaipanla_zhaban`")
            for sql in sqls:
                #print(sql)
                dbConnection.Execute(sql)
        else:
            print(f'''{date} data is None''')
    return ret

def RequestDieTingDataByDates(dates,dbConnection): #跌停数据
    KaiPanLaDieTingListParam = {
        "urlOfToday":"https://apphq.longhuvip.com/w1/api/index.php",
                             
        "queryStringOfToday":"Filter=0&FilterGem=0&FilterMotherboard=0&FilterTIB=0&Index=0&Is_st=1&Order=0&PhoneOSNew=2&PidType=3&Type=4&VerSion=5.11.0.3&a=DaBanList&apiv=w33&c=HomeDingPan&st=10",
        "hostOfToday":"apphq.longhuvip.com",
        #====================
        "urlOfHistory":"https://apphis.longhuvip.com/w1/api/index.php",
        "queryStringOfHistory" :"Day={0}&Filter=0&FilterGem=0&FilterMotherboard=0&FilterTIB=0&Index=0&Is_st=1&Order=0&PhoneOSNew=2&PidType=3&Type=4&VerSion=5.11.0.3&a=HisDaBanList&apiv=w33&c=HisHomeDingPan&st=100",
        "hostOfHistory":"apphis.longhuvip.com"
        }
   
    for date in dates:
        print(f"=======start to request 跌停 data of: {date}=================")
        dataMgr = CKaiPanLaMultiPageDataMgr()
        df = dataMgr.RequestData(date,KaiPanLaDieTingListParam)
        if df is not None and not df.empty:
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
        "queryStringOfToday":"Filter=0&FilterGem=0&FilterMotherboard=0&FilterTIB=0&Index=0&Is_st=1&Order=0&PhoneOSNew=2&PidType=4&Type=6&VerSion=5.11.0.3&a=DaBanList&apiv=w33&c=HomeDingPan&st=100",
        "hostOfToday":"apphq.longhuvip.com",
        #====================
        "urlOfHistory":"https://apphis.longhuvip.com/w1/api/index.php",
        "queryStringOfHistory" :"Day={0}&Filter=0&FilterGem=0&FilterMotherboard=0&FilterTIB=0&Index=0&Is_st=1&Order=0&PhoneOSNew=2&PidType=4&Type=6&VerSion=5.11.0.3&a=HisDaBanList&apiv=w33&c=HisHomeDingPan&st=100",
        "hostOfHistory":"apphis.longhuvip.com"
        }
   
    for date in dates:
        print(f"=======start to request 自然涨停 data of: {date}=================")
        dataMgr = CKaiPanLaMultiPageDataMgr()
        df = dataMgr.RequestData(date,KaiPanLaZhiranZhangTingListParam)
        if df is not None and not df.empty:
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

def RequestIndexData(dates,dbConnection): #大盘指数数据
    IndexListParam = {
        "urlOfToday":"https://apphq.longhuvip.com/w1/api/index.php",
        "queryStringOfToday":"DeviceID=72697ee95ed4399fac9914eba97c8ede3bfddb7c&PhoneOSNew=2&StockIDList=SH000001%2CSZ399001%2CSZ399006%2CSH000688&Token=919d7846d93da295c163371c85cfd81c&UserID=1585460&VerSion=5.11.0.3&a=RefreshStockList&apiv=w33&c=UserSelectStock",
        "hostOfToday":"apphq.longhuvip.com",
        #====================
        "urlOfHistory":"https://apphis.longhuvip.com/w1/api/index.php",
        "queryStringOfHistory" :"Day={0}&PhoneOSNew=2&VerSion=5.11.0.3&a=GetZsReal&apiv=w33&c=StockL2History",
        "hostOfHistory":"apphis.longhuvip.com"
        }
    dataMgr = CKaiPanLaDataMgr()
    all = []
    for date in dates:
        print(f"=======start to request 指数 data of: {date}=================")
        result = dataMgr.RequestData(date,IndexListParam)
        if result is not None:
            js =json.loads(result)
            StockLists = js["StockList"]
            for item in StockLists:
                item["date"] = date
                all.append(item)
        else:
            print(f'''{date} data is None''')

    column = ["date","StockID","prod_name","increase_amount","increase_rate","last_px","turnover"]
    df = pd.DataFrame(all,columns= column)
    print(df)
    sqls = DataFrameToSqls_INSERT_OR_IGNORE(df,"`stock`.`kaipanla_index`")
    for sql in sqls:
        #print(sql)
        dbConnection.Execute(sql)



