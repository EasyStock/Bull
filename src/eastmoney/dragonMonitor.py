
from tokenize import group
import pandas as pd
DRAGON_CONFIG = [
    {
        "name":"陈小群",
        "threshold":0.51,
        "value":{
            "10657404":"中国银河证券股份有限公司大连金马路证券营业部",
            "10787117":"申港证券股份有限公司深圳分公司",
            "10747429":"申港证券股份有限公司深圳深南东路证券营业部",
            "10948136":"申港证券股份有限公司浙江分公司",  #陈小群好基友老余
        },
        "description":"陈小群 和他的好基友们",
    },
    {
        "name":"西湖国贸+上海浦东-喜欢做药",
        "threshold":0.51,
        "value":{
            "10656871":"财信证券股份有限公司杭州西湖国贸中心证券营业部",
            "10944812":"天风证券股份有限公司上海浦东分公司",
        },
        "description":"喜欢做药,华润双鹤，新华制药，雅本化学，奥翔药业，新和成",
    },
    {
        "name":"兴业证券群",
        "threshold":0.34,
        "value":{
            "10637987":"兴业证券股份有限公司上海陆家嘴证券营业部",
            "10027015":"兴业证券股份有限公司上海天钥桥路证券营业部",
            "10056120":"兴业证券股份有限公司武汉新华路证券营业部",
            # "10143566":"平安证券股份有限公司深圳深南东路罗湖商务中心证券营业部",
            # "10028472":"东方财富证券股份有限公司上海东方路证券营业部",
            # "10025094":"国泰君安证券股份有限公司石家庄裕华东路证券营业部",

        },
        "description":" 中路股份",
    },
    # {
    #     "name":"量化基金",
    #     "threshold":0.34,
    #     "value":{
    #         "10088113":"中国国际金融股份有限公司上海分公司",
    #         "10028451":"华泰证券股份有限公司总部",
    #         "10110478":"瑞银证券有限责任公司上海花园石桥路证券营业部",
    #     },
    #     "description":" 量化基金",
    # },
    {
        "name":"北京帮，很多时候都出现顶点点当天或者前一天，看到他们应该及时逃离",
        "threshold":0.51,
        "value":{
            "10668449":"万和证券股份有限公司北京分公司",
            "10484994":"东莞证券股份有限公司北京分公司",
        },
        "description":"成功率不高",
    },
    
]


class CDragonMonitor(object):
    def __init__(self,dbConnection,tableName,tradingDay) -> None:
        self.dbConnection = dbConnection
        self.tradingDay = tradingDay
        self.tableName = tableName

    def DoMonitor(self):
        self.Monitor()
        self.Monitor2()


    def Monitor2(self):
        #东北证券股份有限公司绍兴金柯桥大道证券营业部,中国银河证券股份有限公司大连金马路证券营业部,财信证券股份有限公司杭州西湖国贸中心证券营业部,天风证券股份有限公司上海浦东分公司
        sql = f'''select * from `stock`.`dragon` where operator_ID in (10638005,10657404,10656871,10944812,10948136) and date = "{self.tradingDay}"; '''
        data, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(data,columns=columns)
        print(df)


    def Monitor(self):
        for config in DRAGON_CONFIG:
            name = config["name"]
            Threashold = config["threshold"]
            value = config["value"]
            IDs = tuple(value.keys())
            sql = f'''select * from {self.tableName} where operator_ID in {IDs}  and date = "{self.tradingDay}";'''
            data, columns = self.dbConnection.Query(sql)
            df = pd.DataFrame(data,columns=columns)
            groupByStockID = df.groupby(["stockID",]).count()
            
            stockIDs = list(groupByStockID.index)
            for stockID in stockIDs:
                stockIDDataFrame =df[df["stockID"] == stockID]
                groupByoperator_ID = stockIDDataFrame.groupby(["operator_ID",]).count()
                count = groupByoperator_ID.shape[0]
                ratio = count / len(IDs)
                if ratio > Threashold:
                    sql1 = f'''select * from {self.tableName} where operator_ID in {IDs} and date = "{self.tradingDay}" and stockID = "{stockID}";'''
                    data1, columns1 = self.dbConnection.Query(sql1)
                    df1 = pd.DataFrame(data1,columns=columns1)
                    s = f'''\n============={name}: 匹配度:{ratio*100}% ===========\n\n席位信息: \n'''
                    operator_IDs = list(groupByoperator_ID.index)
                    for operator_ID in operator_IDs:
                        s = s + f'''席位名称:{value[operator_ID]}, 席位编号:{operator_ID} \n'''
                    
                    s= s + "\n"+ str(df1) + "\n"
                    print(s)
                    #input()


if __name__ == "__main__":
    monitor = CDragonMonitor(None,"","2022-07-01")
    monitor.Monitor()

