
import pandas as pd
import logging
import numpy as np

class CVMADataTraining(object):
    def __init__(self,dbConnection,stockID= None):
        self.stockID = stockID
        self.dbConnection = dbConnection
        self.df = None

    def _calcCangWei(self,gailv,yingkui):
        #根据凯利公式计算仓位
        gailv = float(gailv)/100.0
        yingkui = float(yingkui)/100.0
        caili = (gailv*yingkui - (1-gailv))/yingkui*100
        return float(f'''{caili:.1f}''')
    
    def _CalcGaiLv(self,zhangDiefu,vma,vmaThreshold,NDays,gailvThreshold = 70):
        res = self.df[self.df[vma] >= vmaThreshold]
        if res.empty:
            return(False,())
        key = f'''{NDays}日后涨幅'''
        df = res[res[key] >0]
        df = df[df["涨跌幅"]>=zhangDiefu]
        gailv = 100.0*df.shape[0]/res.shape[0]
        zhangfuAvg = float(df[key].mean())
        cangwei = self._calcCangWei(gailv,zhangfuAvg)
        message = f'''{self.stockID}: {vma}>={vmaThreshold:.2f} 涨幅>={zhangDiefu:.2f}%: {key} 大于0的概率:{gailv:.2f}%, 平均涨跌幅:{zhangfuAvg:.2f}%, 仓位:{cangwei}\n'''
        sql = f'''REPLACE INTO `stockdailyinfo_traning_result` (`stockID`, `VMA`,`VMA值`, `涨幅`, `几日后涨幅`, `概率`, `平均涨幅`,`仓位`) VALUES ('{self.stockID}', '{vma}', '{vmaThreshold:.2f}','{zhangDiefu:.2f}', '{key}', '{gailv:.2f}', '{zhangfuAvg:.2f}',{cangwei});'''
        if gailv >=gailvThreshold and cangwei > 0:
            self.dbConnection.Execute(sql)
            logging.error(message)
            return (True,(zhangDiefu,vma,vmaThreshold,key,gailv,sql,message))
        else:
            #logging.info(message)
            return (False,(zhangDiefu,vma,vmaThreshold,key,gailv,sql,message))
        
    def _CalcVMAN(self,quantile,zhangDiefu,N,gailvThreshold = 70):
        key = f'''V/MA{N}'''
        VMANs = list(quantile[key])
        for vmaN in VMANs:
            r1 = self._CalcGaiLv(zhangDiefu,key,vmaN,1,gailvThreshold)
            r3 = self._CalcGaiLv(zhangDiefu,key,vmaN,3,gailvThreshold)
            r5 = self._CalcGaiLv(zhangDiefu,key,vmaN,5,gailvThreshold)
            r7 = self._CalcGaiLv(zhangDiefu,key,vmaN,7,gailvThreshold)

    def Training(self,VMA = 60,gailvThreshold = 70):
        key = f'''V/MA{VMA}'''
        sql = f'''
                SELECT `日期`,`涨跌幅`,`{key}`,`1日后涨幅`,`3日后涨幅`,`5日后涨幅`,`7日后涨幅` FROM stock.stockdailyinfo_traning where `股票代码` = "{self.stockID}";
               '''
        results, columns = self.dbConnection.Query(sql)
        self.df = pd.DataFrame(results,columns=columns)
        self.df[key] = self.df[key].astype(float)

        self.df['涨跌幅'] = self.df['涨跌幅'].astype(float)
        self.df['1日后涨幅'] = self.df['1日后涨幅'].astype(float)
        self.df['3日后涨幅'] = self.df['3日后涨幅'].astype(float)
        self.df['5日后涨幅'] = self.df['5日后涨幅'].astype(float)
        self.df['7日后涨幅'] = self.df['7日后涨幅'].astype(float)
        self.df.dropna(inplace = True)
        self.df.set_index("日期",drop=True,inplace=True)
        steps = np.linspace(0, 1, 51)
        quantile = self.df.quantile(steps)
        ZhangDieFus= list(quantile["涨跌幅"])
        for zhangDiefu in ZhangDieFus:
            self._CalcVMAN(quantile,zhangDiefu,VMA,gailvThreshold)

    
    def UpdateCangwei(self):
        sql = f'''SELECT * FROM stock.stockdailyinfo_traning_result where `仓位` is NULL limit 500000;'''
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)
        df['仓位'] = df.apply(lambda row: self._calcCangWei(row['概率'],row['平均涨幅']), axis=1)
        df['仓位'] = df[df['仓位'] > 0]
        sqls = []
        #size = df.shape[0]
        for _, row in df.iterrows():
            sql = f'''UPDATE `stock`.`stockdailyinfo_traning_result` SET `仓位` = '{row["仓位"]}' WHERE (`stockID` = '{row["stockID"]}') and (`VMA` = '{row["VMA"]}') and (`涨幅` = '{row["涨幅"]}') and (`几日后涨幅` = '{row["几日后涨幅"]}') and (`VMA值` = '{row["VMA值"]}');'''
            sqls.append(sql)
        
        step = 300
        groupedSql = [" ".join(sqls[i:i+step]) for i in range(0,len(sqls),step)]
        for sql in groupedSql:
            self.dbConnection.Execute(sql)

