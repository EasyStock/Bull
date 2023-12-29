import pandas as pd


class CZhuanQianXiaoXing(object):
    def __init__(self, dbConnection,yestoday, today):
        self.dbConnection = dbConnection
        self.yestoday = yestoday
        self.today = today
        self.zhangTing_yestoday = None
        self.zhangTing_today = None
        
        self.jingjiRatio_1 = 0
        self.hongpanRatio_1 =0

        self.jingjiRatio_2 = 0
        self.hongpanRatio_2 = 0

        self.jingjiRatio_3 = 0
        self.hongpanRatio_3 = 0

        self.jingjiRatio_4 = 0
        self.hongpanRatio_4 = 0

        self.shoubanFenbu = 0
        self.lianbanFenbu = 0
        self.zhangTingFenbuStr = ""


        self.countOfZhangTing = 0
        self.countOfLianBan = 0
        self.countOfLessNegative5 = 0
        self.hongpanRatio_Dapan = 0
        self.shiNengKuiQiaoRatio = 0


        self.shiNeng = 0
        self.dongNeng = 0

        self.fuPanStr = []
        self.shiNengStr = []
        self.dongNengStr = []

        self.gaoKaiDiZou = None

        self.dongNeng_shouBanDaMianRatio = 0
        self.dongNeng_shouBanHongPanRatio = 0
        self.dongNeng_lianBanHongPanRatio = 0
        self.dongNeng_lianBanRatio = 0
        self.dongNeng_lianBanDaMianRatio = 0
        self.dongNeng_lianBanWeiZhangtingLvpanRatio = 0
        self.fuPanBiji = ""
        self.daMianData = None
        self.beizhu = ""


    def zhuanqianxiaoying_yestoday(self):
        # 昨日的赚钱效应， 即昨日涨停股票今天的表现
        sql = f'''SELECT A.*,B.`股票简称`, B.`连续涨停天数` As `昨日连续涨停天数`,B.`涨停原因类别` As `昨日涨停原因类别` FROM stock.stockdailyinfo As A, (SELECT `股票代码`,`股票简称`,`连续涨停天数`,`涨停原因类别` FROM stock.stockzhangting where `日期` = "{self.yestoday}") As B where A.`日期` = "{self.today}" and A.`股票代码` = B.`股票代码` and A.`股票代码` REGEXP '^60|^00' order by B.`连续涨停天数` DESC;'''
        print(sql)
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)
        df["涨跌幅"] = df["涨跌幅"].astype('double')
        df["昨日连续涨停天数"] = df["昨日连续涨停天数"].astype('int')
        df["开盘价"] = df["开盘价"].astype('double')
        df["收盘价"] = df["收盘价"].astype('double')
        self.zhangTing_yestoday = df

    
    def zhangTingToday(self):
        # 今天 涨停股
        sql = f'''SELECT * FROM stock.stockzhangting where `日期` = "{self.today}" and `股票代码` REGEXP '^60|^00';'''
        #print(sql)
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)
        df["连续涨停天数"] = df["连续涨停天数"].astype('int')
        self.countOfZhangTing = df.shape[0]
        self.countOfLianBan = df[df["连续涨停天数"] >=2].shape[0]
        self.zhangTing_today = df

        s1 = f'''今日{self.today}涨停数(>40):{self.countOfZhangTing}'''
        s2 = f'''今日{self.today}连板数(>10):{self.countOfLianBan}'''
        self.shiNengStr.append(s1)
        self.shiNengStr.append(s2)
    
    def _shouban(self):
        # 昨日首板晋级率和红盘率
        if self.zhangTing_today is None or self.zhangTing_yestoday is None:
            return
        
        shouban_yes = self.zhangTing_yestoday[self.zhangTing_yestoday["昨日连续涨停天数"] == 1]
        total = shouban_yes.shape[0]
        size1 = shouban_yes[shouban_yes["涨跌幅"] <=0].shape[0]
        size2 = self.zhangTing_today[self.zhangTing_today["连续涨停天数"] == 2].shape[0]

        if total >0:
            self.hongpanRatio_1 = (total-size1)/total
            self.jingjiRatio_1 = size2/total
 
            s1 = f"昨日[{self.yestoday}]共有[{total}]只首板涨停,今日[{self.today}]共[{total-size1}]只红盘，红盘率为:{total-size1}/{total} = {(total-size1)/total:.2f}, 其中[{size2}]只晋级2板,晋级率:{size2}/{total} = {size2/total:.2f}。"
            self.fuPanStr.append(s1)
        else:
            s1 = f"昨日[{self.yestoday}]共有[{total}]只首板涨停,今日[{self.today}]共[{total-size1}]只红盘，红盘率为:{total-size1}/{total} = 0.0, 其中[{size2}]只晋级2板,晋级率:{size2}/{total} = 0.0。"
            self.fuPanStr.append(s1)
     


    def _2Ban(self):
        #昨日2板晋级率和红盘率
        if self.zhangTing_today is None or self.zhangTing_yestoday is None:
            return
        
        ban2 = self.zhangTing_yestoday[self.zhangTing_yestoday["昨日连续涨停天数"] == 2]
        total = ban2.shape[0]
        size1 = ban2[ban2["涨跌幅"] <=0].shape[0]
        size2 = self.zhangTing_today[self.zhangTing_today["连续涨停天数"] == 3].shape[0]

        if total >0:
            self.hongpanRatio_2 = 1.0*(total-size1)/total
            self.jingjiRatio_2 = 1.0*size2/total

            s2 = f"昨日[{self.yestoday}]共有[{total}]只2板涨停,今日[{self.today}]共[{total-size1}]只红盘，红盘率为:{total-size1}/{total} = {(total-size1)/total:.2f}, 其中[{size2}]只晋级3板,晋级率:{size2}/{total} = {size2/total:.2f}。"
            self.fuPanStr.append(s2)
        else:
            s2 = f"昨日[{self.yestoday}]共有[{total}]只2板涨停,今日[{self.today}]共[{total-size1}]只红盘，红盘率为:{total-size1}/{total} = 0.0, 其中[{size2}]只晋级3板,晋级率:{size2}/{total} =0.0。"
            self.fuPanStr.append(s2)


    def _3Ban(self):
        #昨日3板晋级率和红盘率
        if self.zhangTing_today is None or self.zhangTing_yestoday is None:
            return
        
        ban3 = self.zhangTing_yestoday[self.zhangTing_yestoday["昨日连续涨停天数"] == 3]
        total = ban3.shape[0]
        size1 = ban3[ban3["涨跌幅"] <=0].shape[0]
        size2 = self.zhangTing_today[self.zhangTing_today["连续涨停天数"] == 4].shape[0]

        if total>0:
            self.hongpanRatio_3 = (total-size1)/total
            self.jingjiRatio_3 = size2/total

            s3 = f"昨日[{self.yestoday}]共有[{total}]只3板涨停,今日[{self.today}]共[{total-size1}]只红盘，红盘率为:{total-size1}/{total} = {(total-size1)/total:.2f}, 其中[{size2}]只晋级4板,晋级率:{size2}/{total} = {size2/total:.2f}。"
            self.fuPanStr.append(s3)
        else:
            s3 = f"昨日[{self.yestoday}]共有[{total}]只3板涨停,今日[{self.today}]共[{total-size1}]只红盘，红盘率为:{total-size1}/{total} = {0.0}, 其中[{size2}]只晋级4板,晋级率:{size2}/{total} = {0.0}。"
            self.fuPanStr.append(s3)


    def _4BanAndMore(self):
        #昨日4板 及以上 晋级率和红盘率
        if self.zhangTing_today is None or self.zhangTing_yestoday is None:
            return
        
        ban4 = self.zhangTing_yestoday[self.zhangTing_yestoday["昨日连续涨停天数"] >= 4]
        total = ban4.shape[0]
        size1 = ban4[ban4["涨跌幅"] <=0].shape[0]
        size2 = self.zhangTing_today[self.zhangTing_today["连续涨停天数"] >= 5].shape[0]

        if total>0:
            self.hongpanRatio_4 = (total-size1)/total
            self.jingjiRatio_4 = size2/total

            s4 = f"昨日[{self.yestoday}]共有[{total}]只4板及以上涨停,今日[{self.today}]共[{total-size1}]只红盘，红盘率为:{total-size1}/{total} = {(total-size1)/total:.2f}, 其中[{size2}]只晋级5板及以上,晋级率:{size2}/{total} = {size2/total:.2f}。"
            self.fuPanStr.append(s4)
        else:
            s4 = f"昨日[{self.yestoday}]共有[{total}]只4板及以上涨停,今日[{self.today}]共[{total-size1}]只红盘，红盘率为:{total-size1}/{total} = 0.0, 其中[{size2}]只晋级5板及以上,晋级率:{size2}/{total} = 0.0。"
            self.fuPanStr.append(s4)      
  
    
    def zhangTingFenbu(self):
        if self.zhangTing_today is None:
            return

        total = self.zhangTing_today.shape[0]
        size1 = self.zhangTing_today[self.zhangTing_today["连续涨停天数"] == 1].shape[0]

        if total>0:
            self.shoubanFenbu = size1/total
            self.lianbanFenbu = (total-size1)/total

        z= f"今日[{self.today}]共有[{total}]只涨停，首板率:{size1}/{total} = {self.shoubanFenbu:.2f},连板率:{total-size1}/{total} = {self.lianbanFenbu:.2f}。"
        self.fuPanStr.append(z)


    def UpdateDataToDB(self):
        sql = f'''UPDATE `stock`.`fupan`
                SET
                `首板率` = {self.shoubanFenbu:.2f},
                `连板率` = {self.lianbanFenbu:.2f},
                `昨日首板晋级率` = {self.jingjiRatio_1:.2f},
                `昨日首板溢价率` = {self.hongpanRatio_1:.2f},
                `昨日2板晋级率` = {self.jingjiRatio_2:.2f},
                `昨日2板溢价率` = {self.hongpanRatio_2:.2f},
                `昨日3板晋级率` = {self.jingjiRatio_3:.2f},
                `昨日3板溢价率` = {self.hongpanRatio_3:.2f},
                `昨日4板及以上晋级率` = {self.jingjiRatio_4:.2f},
                `昨日4板及以上溢价率` = {self.hongpanRatio_4:.2f},

                `涨停数量` = {self.countOfZhangTing},
                `连板数量` = {self.countOfLianBan},
                `收-5数量` = {self.countOfLessNegative5},
                `大盘红盘比` = {self.hongpanRatio_Dapan:.2f},
                `亏钱效应` = {self.shiNengKuiQiaoRatio:.2f},
                `首板红盘比` = {self.dongNeng_shouBanHongPanRatio:.2f},
                `首板大面比` = {self.dongNeng_shouBanDaMianRatio:.2f},
                `连板股的红盘比` = {self.dongNeng_lianBanHongPanRatio:.2f},
                `连板比例` = {self.dongNeng_lianBanRatio:.2f},
                `连板大面比` = {self.dongNeng_lianBanDaMianRatio:.2f},
                `昨日连板未涨停数的绿盘比` = {self.dongNeng_lianBanWeiZhangtingLvpanRatio :.2f},
                `势能EX` = {self.shiNeng},
                `动能EX` = {self.dongNeng},
                `复盘笔记` = "{self.fuPanBiji}",
                `备注` = "{self.beizhu}"
                WHERE `日期` = "{self.today}";
                '''
        print(sql)
        self.dbConnection.Execute(sql)


    def DailyDataOfToday(self):
        # 10CM 
        sql = f'''select A.*, B.`股票简称`,B.`上市天数` from `stockDailyInfo` AS A,`stockBasicInfo` As B where A.`股票代码`=B.`股票代码` and `日期`= '{self.today}'  and A.`股票代码` REGEXP '^60|^00' and B.`股票简称` not like "%ST%";'''
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)
        df = df[df['涨跌幅'] != '--']
        df["涨跌幅"] = df["涨跌幅"].astype('double')
        df["上市天数"] = df["上市天数"].astype('int')
        df["开盘价"] = df["开盘价"].astype('double')
        df["收盘价"] = df["收盘价"].astype('double')

        df = df[df["上市天数"]>=10] # 去除上市 10天内的新股

        total = df.shape[0]
        self.countOfLessNegative5 = df[df["涨跌幅"]<=-5.0].shape[0]
        s1 = f'''今日{self.today}涨幅在-5%以下个数(<100):{self.countOfLessNegative5}'''
        self.shiNengStr.append(s1)

        if total > 0:
            honpan = df[df["涨跌幅"]>0].shape[0]
            self.hongpanRatio_Dapan = 1.0*honpan/total
            s = f"今日[{self.today}]10CM 大盘共有[{total}]只,其中共[{honpan}]只红盘,红盘比(>0.4):{self.hongpanRatio_Dapan:.2f}。"
            self.shiNengStr.append(s)

        countOFZhangTing = self.zhangTing_today.shape[0]
        self.shiNengKuiQiaoRatio = 1.0* self.countOfLessNegative5 / countOFZhangTing
        
        s2 = f'''今日{self.today}涨幅在-5%以下个数:{self.countOfLessNegative5},涨停个数:{countOFZhangTing}, 两者比值(<2):{self.shiNengKuiQiaoRatio:.2f}'''
        self.shiNengStr.append(s2)

        


    def CalcShiNenng(self):
        self.shiNeng = 0

        # 1. 涨停股数量 大于40 
        if self.countOfZhangTing >=40:
            self.shiNeng = self.shiNeng + 2
        else:
            self.shiNeng = self.shiNeng - 2


        # 2. 连板数量 大于10 
        if self.countOfLianBan >=10:
            self.shiNeng = self.shiNeng + 2
        else:
            self.shiNeng = self.shiNeng - 2

        # 3. 收盘跌幅 在-5% 以下的数量小于100

        if self.countOfLessNegative5 <100:
            self.shiNeng = self.shiNeng + 2
        else:
            self.shiNeng = self.shiNeng - 2

        # 4. 大盘红盘比 大于0.4
        if self.hongpanRatio_Dapan > 0.4:
            self.shiNeng = self.shiNeng + 2
        else:
            self.shiNeng = self.shiNeng - 2   

        # 5. 亏钱效应 = 收盘跌幅在 -5% 以下的个股 / 涨停数量 
        if self.shiNengKuiQiaoRatio <2:
            self.shiNeng = self.shiNeng + 2
        else:
            self.shiNeng = self.shiNeng - 2 

    def CalcDongNeng(self):
        if self.zhangTing_yestoday is None:
            return

        # 1. 计算昨日首板红盘比：
        zhangTing_yestoday_shouban = self.zhangTing_yestoday[self.zhangTing_yestoday["昨日连续涨停天数"] == 1]
        total_shouban = zhangTing_yestoday_shouban.shape[0]
        hongpan_shouban = zhangTing_yestoday_shouban[zhangTing_yestoday_shouban["涨跌幅"] >0].shape[0]
        self.dongNeng_shouBanHongPanRatio = 0
        if total_shouban != 0:
            self.dongNeng_shouBanHongPanRatio = 1.0*hongpan_shouban/total_shouban
        s1 = f'''昨日{self.yestoday}首板涨停, 今日首板红盘比(>0.6): {hongpan_shouban}/{total_shouban} = {self.dongNeng_shouBanHongPanRatio:.2f}'''
        self.dongNengStr.append(s1)
        if self.dongNeng_shouBanHongPanRatio > 0.6:
            self.dongNeng = self.dongNeng + 2
        else:
            self.dongNeng = self.dongNeng -2


        df2 = self.zhangTing_yestoday[self.zhangTing_yestoday["涨跌幅"]>=-5]
        self.gaoKaiDiZou = df2 [(df2["开盘价"] - df2["收盘价"])*(df2["涨跌幅"]/100+1)/df2["收盘价"] >0.05]  #即高开低走的票

        # 2. 计算昨日首板大面比
        shouban_count1 = zhangTing_yestoday_shouban[zhangTing_yestoday_shouban["涨跌幅"]<-5].shape[0]
        shouban_count2 = self.gaoKaiDiZou[self.gaoKaiDiZou["昨日连续涨停天数"] == 1 ].shape[0]
        #print(self.gaoKaiDiZou[self.gaoKaiDiZou["昨日连续涨停天数"] == 1 ])

        self.dongNeng_shouBanDaMianRatio  = 0
        if total_shouban !=0:
            self.dongNeng_shouBanDaMianRatio = 1.0* (shouban_count1+shouban_count2) / total_shouban 
        s2 = f"昨日{self.yestoday}首板涨停, 今日跌幅大于5%共:{shouban_count1}只, 高开低走的共:{shouban_count2}, 今日首板大面比(<0.3): {shouban_count1+shouban_count2}/{total_shouban} = {self.dongNeng_shouBanDaMianRatio:.2f}"
        self.dongNengStr.append(s2)
        if self.dongNeng_shouBanDaMianRatio < 0.3:
            self.dongNeng = self.dongNeng + 2
        else:
            self.dongNeng = self.dongNeng - 2

        
        # 3. 连板股的红盘比
        dianban = self.zhangTing_yestoday[self.zhangTing_yestoday["昨日连续涨停天数"] >= 2]
        lianban_hongpan = dianban[dianban['涨跌幅'] > 0].shape[0]
        lianban_total = dianban.shape[0]
        self.dongNeng_lianBanHongPanRatio = 0
        if lianban_total != 0:
            self.dongNeng_lianBanHongPanRatio = 1.0*lianban_hongpan/lianban_total
        s3 = f'''昨日{self.yestoday}连板涨停, 今日连板红盘比(>0.6): {lianban_hongpan}/{lianban_total} = {self.dongNeng_lianBanHongPanRatio:.2f}'''
        self.dongNengStr.append(s3)

        if self.dongNeng_lianBanHongPanRatio >=0.6:
            self.dongNeng = self.dongNeng +2
        else:
            self.dongNeng  = self.dongNeng -2


        #4. 连板比例: 今日连板总数/昨日连板总数
        zhangTing_Today_lianban = self.zhangTing_today[self.zhangTing_today["连续涨停天数"] >= 2]
        count_lianban_today = zhangTing_Today_lianban.shape[0]
        self.dongNeng_lianBanRatio = 0
        if lianban_total !=0:
            self.dongNeng_lianBanRatio = count_lianban_today/lianban_total
        s4 = f'''连板比例(>0.6):{count_lianban_today}/{lianban_total} = {self.dongNeng_lianBanRatio:.2f}'''
        self.dongNengStr.append(s4)
        if self.dongNeng_lianBanRatio >0.6:
            self.dongNeng = self.dongNeng +2
        else:
            self.dongNeng  = self.dongNeng -2

        #5. 计算昨日连板大面比
        lianban_count1 = dianban[dianban["涨跌幅"] < -5].shape[0]
        lianban_count2 = self.gaoKaiDiZou[self.gaoKaiDiZou["昨日连续涨停天数"] >= 2].shape[0]
        self.dongNeng_lianBanDaMianRatio = 0
        if lianban_total !=0:
            self.dongNeng_lianBanDaMianRatio = 1.0* (lianban_count1+lianban_count2) / lianban_total 
        s5 = f"昨日{self.yestoday}连板涨停, 今日跌幅大于5%共:{lianban_count1}只, 高开低走的共:{lianban_count2}, 今日连板大面比(<0.3): {lianban_count1+lianban_count2}/{lianban_total} = {self.dongNeng_lianBanDaMianRatio:.2f}"
        self.dongNengStr.append(s5)
        if self.dongNeng_lianBanDaMianRatio < 0.3:
            self.dongNeng = self.dongNeng + 2
        else:
            self.dongNeng = self.dongNeng - 2

        #6. 昨日连扳未涨停股的绿盘比
        lvpan_size = dianban[dianban["涨跌幅"] < 0].shape[0]
        size1 = self.zhangTing_today[self.zhangTing_today['连续涨停天数']>=3].shape[0] # 昨日连板今日涨停数
        if (lianban_total - size1) == 0:
            self.dongNeng_lianBanWeiZhangtingLvpanRatio = 0
        else:
            self.dongNeng_lianBanWeiZhangtingLvpanRatio = lvpan_size/(lianban_total - size1)
        s6 = f"昨日{self.yestoday}连板涨停,今日未涨停的绿盘数比(<0.5): {lvpan_size}/{lianban_total - size1} = {self.dongNeng_lianBanWeiZhangtingLvpanRatio:.2f}"
        self.dongNengStr.append(s6)
        if self.dongNeng_lianBanWeiZhangtingLvpanRatio < 0.5:
            self.dongNeng = self.dongNeng + 2
        else:
            self.dongNeng = self.dongNeng - 2

    
    def isGaoChao(self):
        if (self.shiNeng == 10 and self.dongNeng == 12) or (self.dongNeng_shouBanHongPanRatio >0.78 and self.dongNeng_lianBanHongPanRatio >0.78):
            self.dongNengStr.append("今日短线情绪《高潮》了！！！！")
            self.beizhu = "高潮"
        elif self.dongNeng_lianBanHongPanRatio >0.75:
            self.dongNengStr.append("今日短线情绪 《半高潮》了！！！！")
            self.beizhu = "半高潮"
        elif self.dongNeng == -12 and self.shiNeng <= -2:
            self.dongNengStr.append("今日短线情绪 《冰点》了！！！！")
            self.beizhu = "冰点"
            

    def WriteFuPanBiJi(self):

        s1 = '\n'.join(self.fuPanStr)
        s2 = '\n'.join(self.shiNengStr)
        s3 = '\n'.join(self.dongNengStr)
        s4 = str(list(self.gaoKaiDiZou["股票简称"]))
        self.fuPanBiji = f'''
\n\n\n====================复盘笔记{self.today}=======================

1. ============复盘记录====================
{s1}


2. ============势能计算过程记录====================
{s2}


3. ============动能计算过程记录=======================
{s3}


4. ============昨日涨停，今天高开低走的股票====================
{s4}

4. ============今日大盘势能/动能====================
势能: {self.shiNeng}
动能: {self.dongNeng}

        '''
       
    def PrintString(self):
        print(self.fuPanBiji)
        print("============昨日涨停，今天高开低走的股票====================")
        print(self.gaoKaiDiZou)
        print("============昨日涨停，今天大面的股票====================")
        print(self.zhangTing_yestoday[self.zhangTing_yestoday["涨跌幅"]<-5])


################################################################
    def _format2To3(self,row):
        ban3 = row['3连板个数']
        zuoRi2Ban = row['昨日2板数']
        ratio = 0.0
        if zuoRi2Ban == 0:
            ratio = 0.0
        else:
            ratio = float(f'''{ban3/zuoRi2Ban:.2f}''')
        
        return ratio

    def _format3ToHigher(self,row):
        ban4 = row['4连板及以上个数']
        ban3_zuori = row['昨日3板数']
        ban4_zuori = row['昨日4板及以上个数']
        ratio = 0.0
        if (ban3_zuori + ban4_zuori) == 0:
            ratio = 0.0
        else:
            ratio = float(f'''{ban4/(ban3_zuori + ban4_zuori):.2f}''')
        return ratio


    def _DataFrameToSqls_UPDATE(self,datas,tableName,index_str):
        sqls = []
        for index, row in datas.iterrows():
            sql = '''UPDATE %s SET ''' %(tableName)

            for rowIndex, value in row.items():
                sql = sql + '''`%s` = '%s',''' %(rowIndex,value)
            sql = sql[:-1]
            sql = sql + ''' WHERE `%s` = '%s'; '''%(index_str,index)
            sqls.append(sql)
        return sqls

    def UpdateRatios(self):
        #更新2进3成功率
        #更新3进更高成功率
        sql =f'''SELECT * FROM stock.fupan;'''
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)

        df["昨日2板数"] = df["2连板个数"].shift()
        df["昨日3板数"] = df["3连板个数"].shift()
        df["昨日4板及以上个数"] = df["4连板及以上个数"].shift()

        df['2进3成功率'] = df.apply(lambda row: self._format2To3(row), axis=1)
        df['3进高成功率'] = df.apply(lambda row: self._format3ToHigher(row), axis=1)
        newDf = pd.DataFrame(df,columns=['日期','2进3成功率','3进高成功率'])
        newDf.set_index(["日期",],drop=True,inplace=True)
        sqls = self._DataFrameToSqls_UPDATE(newDf,tableName='fupan',index_str='日期')
        for sql in sqls[-7:]:
            self.dbConnection.Execute(sql)
            print(sql)
################################################################

     
    def _isGaoCaoOrBanGaoCao(self,df):
        #高潮: 动能综合值=12 且 势能综合值=10 或者 连板股的红盘比 >=0.78 首板股的红盘比 >=0.78
        #半高潮: 只有 连板股的红盘比 >=0.78
        lastRow = df.iloc[-1]
        if int(lastRow['动能EX']) == 12 and int(lastRow['势能EX']) == 10:
            return "高潮"
        
        if float(lastRow['连板股的红盘比']) >= 0.75 and float(lastRow['首板红盘比']) >= 0.75:
            return "高潮"
        
        if lastRow['连板股的红盘比'] >= 0.75:
            return "半高潮"

        return ""

    def __bingDian(self,row):
        if int(row['动能EX']) <= -8 and int(row['势能EX']) <= -2:
            return True
        
        return False

    def __bingDian2(self,row):
        if row['首板红盘比'] < 0.4 and row['连板股的红盘比'] <= 0.4:
            return True
        
        return False
    
    def _isBingDian(self,df):
        lastRow1 = df.iloc[-1]
        lastRow2 = df.iloc[-2]
        lastRow3 = df.iloc[-3]
        lastRow4 = df.iloc[-4]

        #冰点期判断 - 强势行情: 如果动能综合值 =-12 且 势能综合值 <=-2 或者 (动能综合值<=-8 且 势能综合值<=-2) 出现两次
        if int(lastRow1['动能EX']) == -12 and int(lastRow1['势能EX']) <= -2:
            return "强势行情-冰点"
        
        r = [row for row in (lastRow1,lastRow2,lastRow3,lastRow4) if self.__bingDian(row) == True]
        if len(r) >= 2:
            return "强势行情-冰点"
        
        #冰点期判断 - 弱势行情: 如果动能综合值 <=-8 且 势能综合值 =-10 且首板赚钱效应和连板赚钱效应都出现过 <0.4 或者 连续两天动能综合值和势能综合值都<=-6
        s = [row for row in (lastRow1,lastRow2,lastRow3,lastRow4) if self.__bingDian2(row) == True]
        if int(lastRow1['动能EX']) <= -8 and int(lastRow1['势能EX']) == -10 and len(s)>=1:
            return "弱势行情-冰点"
        
        if int(lastRow1['动能EX']) <= -6 and int(lastRow1['势能EX']) <= -6 and int(lastRow2['动能EX']) <= -6 and int(lastRow2['势能EX']) <= -6:
            return "弱势行情-冰点"
        
        return ""
        

    def _QingXuHigherThanYestoday(self,df):
        lastRow1 = df.iloc[-1]
        lastRow2 = df.iloc[-2]
        if lastRow1["3进高成功率"] > lastRow2["3进高成功率"]:
            return "情绪比昨日强 - 高位"
        elif lastRow1["3进高成功率"] < lastRow2["3进高成功率"]:
            return "情绪比昨日弱 - 高位"
        else:
            if lastRow1["2进3成功率"] > lastRow2["2进3成功率"]:
                return "情绪比昨日强 - 低位"
            elif lastRow1["2进3成功率"] < lastRow2["2进3成功率"]:
                return "情绪比昨日弱 - 低位"

        return ""
            
    
    def PanduanQingXu(self):
        '''
            判断情绪:
                高潮: 动能综合值=12 且 势能综合值=10 或者 连板股的红盘比 >=0.78 首板股的红盘比 >=0.78
                半高潮: 只有 连板股的红盘比 >=0.78
                冰点期判断 - 强势行情: 如果动能综合值 =-12 且 势能综合值 <=-2 或者 (动能综合值<=-8 且 势能综合值<=-2) 出现两次
                冰点期判断 - 弱势行情: 如果动能综合值 <=-8 且 势能综合值 =-10 且首板赚钱效应和连板赚钱效应都出现过 <0.4 或者 连续两天动能综合值和势能综合值都<=-6
            今日情绪与昨日情绪相比
                如果 3进高成功率 > 昨日 3进高成功率 ===> 强
                如果 3进高成功率 < 昨日 3进高成功率 ===> 弱
                如果 3进高成功率 = 昨日 3进高成功率
                    2进3成功率 > 昨日 2进3成功率 ===> 强
                    2进3成功率 < 昨日 2进3成功率 ===> 弱
        '''
            
        sql =f'''SELECT * FROM stock.fupan;'''
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)

        gaocao = self._isGaoCaoOrBanGaoCao(df)
        bingdian = self._isBingDian(df)
        qingxu = self._QingXuHigherThanYestoday(df)

        beizhu = ""
        if gaocao!= "":
            beizhu = f"今日情绪:{gaocao};"
        
        if bingdian!= "":
            beizhu = f"今日情绪:{bingdian};"
        
        if qingxu!= "":
            beizhu = f"{beizhu}{qingxu}"
        
        sql = f'''UPDATE `stock`.`fupan` SET `备注` = '{beizhu}' WHERE (`日期` = '{self.today}');'''
        self.dbConnection.Execute(sql)

################################################################
    def ZhuanQianXiaoYing(self):
        self.zhuanqianxiaoying_yestoday()
        self.zhangTingToday()
        self.DailyDataOfToday()

        self.zhangTingFenbu()
        self._shouban()
        self._2Ban()
        self._3Ban()
        self._4BanAndMore()
    
        self.CalcShiNenng()
        self.CalcDongNeng()
        self.isGaoChao()
        self.WriteFuPanBiJi()
        self.UpdateDataToDB()
        self.PrintString()
        self.UpdateRatios()
        self.PanduanQingXu()
