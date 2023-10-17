import pandas as pd
import re
from workspace import workSpaceRoot



class CFupanSummary(object):
    def __init__(self,dbConnection,today) -> None:
        self.dbConnection = dbConnection
        self.maxLianBanCount = 0
        self.minLianBanCount = 0
        self.gaoDuBan = 0
        self.today = today
        self.reDianBankuai1 = None
        self.reDianBankuai2 = None
        self.reDianBankuai1DF = None
        self.reDianBankuai2DF = None
        self.lianbanDF = None
        self.shoubanDF = None
        self.markingDatas = None

    def FormatHead(self):
        HEAD = '''
        <html xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office" xmlns:x="urn:schemas-microsoft-com:office:excel" xmlns="http://www.w3.org/TR/REC-html40">
        <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
        <meta name="ProgId" content="Excel.Sheet">
        <meta name="Generator" content="WPS Office ET">
        <!--[if gte mso 9]>
        <xml>
            <o:DocumentProperties>
            <o:Author>mac</o:Author>
            <o:Created>2022-09-02T20:58:15</o:Created>
            <o:LastAuthor>yuchonghuang</o:LastAuthor>
            <o:LastSaved>2023-07-03T19:59:15</o:LastSaved>
            </o:DocumentProperties>
            <o:CustomDocumentProperties>
            <o:ICV dt:dt="string">D86F0C517F52BA45E7FD11639C3183AB_41</o:ICV>
            <o:KSOProductBuildVer dt:dt="string">2052-5.4.0.7910</o:KSOProductBuildVer>
            </o:CustomDocumentProperties>
        </xml>
        <![endif]-->
        <style>
        <!-- @page
            {margin:1.00in 0.75in 1.00in 0.75in;
            mso-header-margin:0.50in;
            mso-footer-margin:0.50in;}
        tr
            {mso-height-source:auto;
            mso-ruby-visibility:none;}
        col
            {mso-width-source:auto;
            mso-ruby-visibility:none;}
        br
            {mso-data-placement:same-cell;}
        .font3
            {color:#00B050;
            font-size:14.0pt;
            font-weight:700;
            font-style:normal;
            text-decoration:none;
            font-family:"宋体";
            mso-generic-font-family:auto;
            mso-font-charset:134;}
        .font4
            {color:windowtext;
            font-size:14.0pt;
            font-weight:400;
            font-style:normal;
            text-decoration:none;
            font-family:"宋体";
            mso-generic-font-family:auto;
            mso-font-charset:134;}
        .font29
            {color:#FF0000;
            font-size:14.0pt;
            font-weight:400;
            font-style:normal;
            text-decoration:none;
            font-family:"宋体";
            mso-generic-font-family:auto;
            mso-font-charset:134;}
        .style0
            {mso-number-format:"General";
            text-align:general;
            vertical-align:middle;
            white-space:nowrap;
            mso-rotate:0;
            color:#000000;
            font-size:11.0pt;
            font-weight:400;
            font-style:normal;
            text-decoration:none;
            font-family:宋体;
            mso-font-charset:134;
            border:none;
            mso-protection:locked visible;
            mso-style-name:"常规";
            mso-style-id:0;}
        .xl65
            {mso-style-parent:style0;
            text-align:center;
            mso-pattern:auto none;
            background:#FFFF00;
            font-size:36.0pt;
            font-weight:700;
            mso-font-charset:134;
            border:.5pt solid windowtext;}

        .xl67
            {mso-style-parent:style0;
            text-align:left;
            font-size:18.0pt;
            mso-font-charset:134;
            border:.5pt solid windowtext;}
        .xl68
            {mso-style-parent:style0;
            text-align:left;
            font-size:18.0pt;
            mso-font-charset:134;
            border:.5pt solid windowtext;}
        .xl69
            {mso-style-parent:style0;
            text-align:left;
            white-space:normal;
            color:#00B050;
            font-size:14.0pt;
            font-weight:700;
            mso-font-charset:134;
            border:.5pt solid windowtext;}
        .xl70
            {mso-style-parent:style0;
            text-align:center;
            mso-pattern:auto none;
            background:#808080;
            mso-font-charset:134;
            border:.5pt solid windowtext;}
        .xl72
            {mso-style-parent:style0;
            text-align:left;
            vertical-align:top;
            white-space:normal;
            color:windowtext;
            font-size:14.0pt;
            mso-font-charset:134;
            border:.5pt solid windowtext;}
        .xl74
            {mso-style-parent:style0;
            text-align:left;
            font-size:18.0pt;
            mso-font-charset:134;
            border-left:.5pt solid windowtext;
            border-right:.5pt solid windowtext;
            border-bottom:.5pt solid windowtext;}
        .xl76
            {mso-style-parent:style0;
            mso-number-format:"yyyy/m/d";
            text-align:left;
            font-size:14.0pt;
            mso-font-charset:134;
            border:.5pt solid windowtext;}
        .xl77
            {mso-style-parent:style0;
            text-align:left;
            font-size:14.0pt;
            mso-font-charset:134;
            border:.5pt solid windowtext;}
        .xl78
            {mso-style-parent:style0;
            text-align:left;
            font-size:14.0pt;
            background:#FFC000;
            mso-font-charset:134;
            border:.5pt solid windowtext;}
        .xl82
            {mso-style-parent:style0;
            text-align:left;
            mso-pattern:auto none;
            background:#808080;
            mso-font-charset:134;
            border-left:.5pt solid windowtext;
            border-top:.5pt solid windowtext;
            border-right:.5pt solid windowtext;}
        .xl89
            {mso-style-parent:style0;
            text-align:left;
            mso-pattern:auto none;
            background:#808080;
            mso-font-charset:134;
            border:.5pt solid windowtext;}
        .xl99
            {mso-style-parent:style0;
            text-align:left;
            mso-pattern:auto none;
            background:#808080;
            mso-font-charset:134;
            border:.5pt solid windowtext;}
        .xl101
            {mso-style-parent:style0;
            text-align:left;
            mso-pattern:auto none;
            background:#BDD7EE;
            font-size:14.0pt;
            mso-font-charset:134;
            border:.5pt solid windowtext;}
        .xl102
            {mso-style-parent:style0;
            mso-number-format:"0\.00%";
            text-align:left;
            font-size:14.0pt;
            mso-font-charset:134;
            border:.5pt solid windowtext;}
        .xl103
            {mso-style-parent:style0;
            text-align:left;
            mso-pattern:auto none;
            background:#BDD7EE;
            font-size:14.0pt;
            mso-font-charset:134;
            color:#FF0000;
            border:.5pt solid windowtext;}
        .xl104
            {mso-style-parent:style0;
            mso-number-format:"0\.00%";
            text-align:left;
            font-size:14.0pt;
            color:#FF0000;
            mso-font-charset:134;
            border:.5pt solid windowtext;}
        .xl105
            {mso-style-parent:style0;
            mso-number-format:"h:mm:ss";
            text-align:left;
            mso-pattern:auto none;
            background:#BDD7EE;
            font-size:14.0pt;
            mso-font-charset:134;
            border:.5pt solid windowtext;}
        .xl106
            {mso-style-parent:style0;
            mso-number-format:"0\.00%";
            text-align:left;
            font-size:16.0pt;
            mso-font-charset:134;
            border:.5pt solid windowtext;}
        .xl107
            {mso-style-parent:style0;
            mso-number-format:"h:mm:ss";
            text-align:left;
            mso-pattern:auto none;
            background:#BDD7EE;
            font-size:14.0pt;
            color:#FF0000;
            mso-font-charset:134;
            border:.5pt solid windowtext;}
        .xl108
            {mso-style-parent:style0;
            mso-number-format:"0\.00%";
            text-align:left;
            font-size:16.0pt;
            color:#FF0000;
            mso-font-charset:134;
            border:.5pt solid windowtext;}

        -->  </style>
        <!--[if gte mso 9]>
        <xml>
            <x:ExcelWorkbook>
            <x:ExcelWorksheets>
            <x:ExcelWorksheet>
            <x:Name>Sheet1</x:Name>
            <x:WorksheetOptions>
                <x:DefaultRowHeight>336</x:DefaultRowHeight>
                <x:StandardWidth>2360</x:StandardWidth>
                <x:Selected/>
                <x:TopRowVisible>10</x:TopRowVisible>
                <x:LeftColumnVisible>6</x:LeftColumnVisible>
                <x:Panes>
                <x:Pane>
                <x:Number>3</x:Number>
                <x:ActiveCol>23</x:ActiveCol>
                <x:ActiveRow>60</x:ActiveRow>
                <x:RangeSelection>X61</x:RangeSelection>
                </x:Pane>
                </x:Panes>
                <x:ProtectContents>False</x:ProtectContents>
                <x:ProtectObjects>False</x:ProtectObjects>
                <x:ProtectScenarios>False</x:ProtectScenarios>
                <x:PageBreakZoom>100</x:PageBreakZoom>
                <x:Print>
                <x:PaperSizeIndex>9</x:PaperSizeIndex>
                </x:Print>
            </x:WorksheetOptions>
            </x:ExcelWorksheet>
            </x:ExcelWorksheets>
            <x:ProtectStructure>False</x:ProtectStructure>
            <x:ProtectWindows>False</x:ProtectWindows>
            <x:SelectedSheets>0</x:SelectedSheets>
            <x:WindowHeight>24320</x:WindowHeight>
            <x:WindowWidth>-16196</x:WindowWidth>
            </x:ExcelWorkbook>
        </xml>
        <![endif]-->
        </head>
        <body link="blue" vlink="purple">
        <table width="13213.10" border="0" cellpadding="0" cellspacing="0" style='width:13213.10pt;border-collapse:collapse;table-layout:fixed;'>
        <col width="47.95" style='mso-width-source:userset;mso-width-alt:2338;'/>
        <col width="109.95" style='mso-width-source:userset;mso-width-alt:5361;'/>
        <col width="89.95"  style='mso-width-source:userset;mso-width-alt:4385;'/>
        <col width="113.35" style='mso-width-source:userset;mso-width-alt:5526;'/>
        <col width="114.15" style='mso-width-source:userset;mso-width-alt:5565;'/>
        <col width="120.80" style='mso-width-source:userset;mso-width-alt:5890;'/>
        <col width="124.15" style='mso-width-source:userset;mso-width-alt:6053;'/>
        <col width="107.45" style='mso-width-source:userset;mso-width-alt:5239;'/>
        <col width="124.60" style='mso-width-source:userset;mso-width-alt:6075;'/>
        <col width="105" style='mso-width-source:userset;mso-width-alt:5119;'/>
        <col width="132.50" style='mso-width-source:userset;mso-width-alt:6460;'/>
        <col width="140.85" style='mso-width-source:userset;mso-width-alt:6867;'/>
        <col width="151.65" style='mso-width-source:userset;mso-width-alt:7394;'/>
        <col width="156.60" style='mso-width-source:userset;mso-width-alt:7635;'/>
        <col width="113.30" style='mso-width-source:userset;mso-width-alt:5524;'/>
        <col width="68.45" style='mso-width-source:userset;mso-width-alt:3337;'/>
        <col width="57.45" style='mso-width-source:userset;mso-width-alt:2801;'/>
        <col width="74.95" style='mso-width-source:userset;mso-width-alt:3654;'/>
        <col width="47.95" span="238" style='mso-width-source:userset;mso-width-alt:2338;'/>
        '''
        return HEAD

    def FormatTitle(self):
        titele = f'''<tr height="45" style='height:45.00pt;mso-height-source:userset;mso-height-alt:900;'>
        <td class="xl65" height="45" width="2040.75" colspan="20" style='height:45.00pt;width:2040.75pt;border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;' x:str>{self.today}复盘</td>
        <td width="47.95" style='width:47.95pt;'></td>
        <td width="47.95" style='width:47.95pt;'></td>
        </tr>
    '''
        return titele


    def FormatWarning(self):
        head = '''
        <tr height="16.80" style='height:16.80pt;'>
        <td class="xl67" height="182.20" colspan="2" rowspan="10" style='height:182.20pt;border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;' x:str>交易八大原则</td>
        <td class="xl69" colspan="18" rowspan="10" style='border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;' x:str>1. 只做龙头不做杂毛，不浪费一颗子弹在杂毛身上，亏钱也要亏在龙头上;<font class="font3"><br/></font><font class="font3">2. 错过永远比错误好，对于模式内要敢于出手，模式外都要学会放弃，勿起贪念；</font><font class="font3"><br/></font><font class="font3">3. 只做周期总龙头和连板总龙头都持有和做T，降低操作频次，提高胜率；</font><font class="font3"><br/></font><font class="font3">4. 高潮期之后次日不开新仓，以持仓股的持有或者清仓为主；</font><font class="font3"><br/></font><font class="font3">5. 衰退期越努力越亏钱，学会空仓比什么都重要；</font><font class="font3"><br/></font><font class="font3">6. 连续三次开仓吃面，一定要休息一天，调整心态；</font><font class="font3"><br/></font><font class="font3">7. 仓位管理一定要根据市场环境都赢面来确定，而不是根据自己 的望断；</font><font class="font3"><br/></font><font class="font3">8. 同一只股，只有第一笔仓位盈利后才能加仓；</font></td>
        <td colspan="233" style='mso-ignore:colspan;'></td>
    </tr>
        '''

        return head

    def _QueryMarkingData(self):
        sql = f'''SELECT * FROM stock.fuPan order by `日期` DESC limit 30;'''
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)
        df = df.iloc[::-1]
        df["涨停数量"] = df["涨停数量"].fillna(0).astype(int)
        df["连板数量"] = df["连板数量"].fillna(0).astype(int)
        df["收-5数量"] = df["收-5数量"].fillna(0).astype(int)
        df["势能EX"] = df["势能EX"].fillna(0).astype(int)
        df["动能EX"] = df["动能EX"].fillna(0).astype(int)
        return df


    def formatVolumn(self,volumn,delta = 1.0):
        newVolumn = float(volumn) * delta
        s = newVolumn /100000000.0 # 除以1亿
        ret = "亿"
        if s <1:
            t = newVolumn / 10000.0
            ret = f'''{t:.0f}万'''
        else:
            ret = f'''{s:.2f}亿'''
        return ret


    def ChengjiaoLiang(self,df):
        df['新成交额'] = df.apply(lambda row: self.formatVolumn(row['成交额'],1.0), axis=1)
        df['成交额4%'] = df.apply(lambda row: self.formatVolumn(row['成交额'],0.04), axis=1)
        df['成交额8%'] = df.apply(lambda row: self.formatVolumn(row['成交额'],0.08), axis=1)
        df['成交额10%'] = df.apply(lambda row: self.formatVolumn(row['成交额'],0.1), axis=1)
        df['流通市值'] = df.apply(lambda row: self.formatVolumn(row['流通市值'],1), axis=1)

    def _GetZhangTingData(self):
        sql = f'''select A.*,B.`成交额`,C.`流通市值`,C.`所属概念` from  stock.stockzhangting AS A,stock.stockdailyinfo As B, stock.stockbasicinfo AS C where A.`日期`  = "{self.today}"  and B.`日期`  = "{self.today}"  and A.`股票代码` = B.`股票代码` and A.`股票代码` = C.`股票代码` order by A.`连续涨停天数` DESC,A.`涨停关键词` DESC,A.`最终涨停时间` ASC;'''
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)
        self.ChengjiaoLiang(df)
        df["连续涨停天数"] = df["连续涨停天数"].astype(int)
        return df

    def _AnalysisZhangTingReason(self):
        df = self._GetZhangTingData()
        #print(df)
        reasons = []
        for index, row in df.iterrows():
            reason = row["涨停原因类别"]
            reasons.extend(reason.split("+"))

        reasons = list(set(reasons))
        reasonResults = {}
        for reason in reasons:
            sql = f"select count(*) from `stockZhangting`AS A, `stockBasicInfo` As B where A.`股票代码`= B.`股票代码` and `日期` = '{self.today}' and A.`涨停原因类别` like '%{reason}%' ;"
            result ,_ = self.dbConnection.Query(sql)
            count = result[0][0]
            reasonResults[reason] = count
    
        ret = sorted(reasonResults.items(), key=lambda d: d[1],reverse=True)
        self.reDianBankuai1 = ret[0][0]
        self.reDianBankuai2 = ret[1][0]
        self.reDianBankuai1DF = df[df["涨停原因类别"].str.contains(self.reDianBankuai1)]
        self.reDianBankuai2DF = df[df["涨停原因类别"].str.contains(self.reDianBankuai2)]
        self.reDianBankuai1DF.reset_index(inplace=True)
        self.reDianBankuai2DF.reset_index(inplace=True)

        self.lianbanDF = df[df["连续涨停天数"]>=2]
        self.shoubanDF = df[df["连续涨停天数"]==1]
        self.lianbanDF.reset_index(inplace=True)
        self.shoubanDF.reset_index(inplace=True)

        # print(self.reDianBankuai1DF)
        # print(self.reDianBankuai2DF)
        for r in ret[:2]:
            if r[1] >=2:
                print(r)

    def FormateSummaryOfToday(self,line1,line2,line3,line4):
        summary = f'''
    <tr height="16.80" style='height:16.80pt;'>
        <td colspan="233" style='mso-ignore:colspan;'></td>
    </tr>
    <tr height="16.80" style='height:16.80pt;'>
        <td colspan="233" style='mso-ignore:colspan;'></td>
    </tr>
    <tr height="16.80" style='height:16.80pt;'>
        <td colspan="233" style='mso-ignore:colspan;'></td>
    </tr>
    <tr height="16.80" style='height:16.80pt;'>
        <td colspan="233" style='mso-ignore:colspan;'></td>
    </tr>
    <tr height="16.80" style='height:16.80pt;'>
        <td colspan="233" style='mso-ignore:colspan;'></td>
    </tr>
    <tr height="16.80" style='height:16.80pt;'>
        <td colspan="233" style='mso-ignore:colspan;'></td>
    </tr>
    <tr height="16.80" style='height:16.80pt;'>
        <td colspan="233" style='mso-ignore:colspan;'></td>
    </tr>
    <tr height="16.80" style='height:16.80pt;'>
        <td colspan="233" style='mso-ignore:colspan;'></td>
    </tr>
    <tr height="31" style='height:31.00pt;mso-height-source:userset;mso-height-alt:620;'>
        <td colspan="233" style='mso-ignore:colspan;'></td>
    </tr>
    <tr height="16.80" style='height:16.80pt;'>
        <td class="xl70" height="16.80" colspan="20" style='height:16.80pt;border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;'></td>
        <td colspan="233" style='mso-ignore:colspan;'></td>
    </tr>
    <tr height="16.80" style='height:16.80pt;'>
        <td class="xl67" height="100.80" colspan="2" rowspan="6" style='height:100.80pt;border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;' x:str>一.总结</td>
        <td class="xl72" colspan="18" rowspan="6" style='border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;' x:str>{line1}<br/>{line2}<br/>{line3}<br/>{line4}</td>
        <td colspan="233" style='mso-ignore:colspan;'></td>
    </tr>
    <tr height="16.80" style='height:16.80pt;'>
        <td colspan="233" style='mso-ignore:colspan;'></td>
    </tr>
    <tr height="16.80" style='height:16.80pt;'>
        <td colspan="233" style='mso-ignore:colspan;'></td>
    </tr>
    <tr height="16.80" style='height:16.80pt;'>
        <td colspan="233" style='mso-ignore:colspan;'></td>
    </tr>
    <tr height="16.80" style='height:16.80pt;'>
        <td colspan="233" style='mso-ignore:colspan;'></td>
    </tr>
    <tr height="16.80" style='height:16.80pt;'>
        <td colspan="233" style='mso-ignore:colspan;'></td>
    </tr>
        '''
        return summary

    def _formatLine1(self,lastRow):
        v = lastRow["两市量"]
        addV = lastRow["增量"]
        hongPan = lastRow["红盘"]
        lvPan = lastRow["绿盘"]
        zhangTing = lastRow["实际涨停"]
        dieTing = lastRow["跌停"]
        zhaBan = lastRow["炸板"]
        zhanBanRatio = lastRow["炸板率"]
        lianBanCount = lastRow["连板"]
        line1 = f'''1. 今日两市成交量:</font><font class="font29">{v}</font><font class="font4">,与昨日相比量能增加:</font><font class="font29">{addV}</font><font class="font4">, 红盘:</font><font class="font29">{hongPan}</font><font class="font4">, 绿盘: </font><font class="font29">{lvPan}</font><font class="font4">，涨停: </font><font class="font29">{zhangTing}</font><font class="font4">, 跌停: </font><font class="font29">{dieTing}</font><font class="font4">，炸板: </font><font class="font29">{zhaBan}</font><font class="font4">，炸板率: </font><font class="font29">{zhanBanRatio}</font><font class="font4">，连板个数: </font><font class="font29">{lianBanCount}</font><font class="font4">，近30个交易日最多连板个数: </font><font class="font29">{self.maxLianBanCount} </font><font class="font4">最少连板个数: </font><font class="font29">{self.minLianBanCount}</font><font class="font4">
        '''
        return line1

    def _formatLine2(self,lastRow):
        ratio10 = lastRow["10CM首板奖励率"]
        ratio20 = lastRow["20CM首板奖励率"]
        if ratio20 == "-200.00":
            ratio20 = "-"
        ratio10_l = lastRow["10CM连板奖励率"]
        ratio20_l = lastRow["20CM连板奖励率"]
        if ratio20_l == "-200.00":
            ratio20_l = "-"
        countStock2 = lastRow["2连板个数"]
        stock3 = lastRow["3连个股"]
        countStock3 = lastRow["3连板个数"]
        stock4 = lastRow["4连及以上个股"]
        countStock4 = lastRow["4连板及以上个数"]
        line2 = f'''2. 今日10CM首板奖励率:</font><font class="font29">{ratio10}%</font><font class="font4">，20CM首板奖励率:</font><font class="font29">{ratio20}%</font><font class="font4">，10CM连板奖励率:</font><font class="font29">{ratio10_l}%</font><font class="font4">，20CM连板奖励率:</font><font class="font29">{ratio20_l}%</font><font class="font4">,2连板个股(</font><font class="font29">{countStock2}个</font>),3连板个股(</font><font class="font29">{countStock3}个</font><font class="font4">): </font><font class="font29">{stock3}</font><font class="font4">， 4连及以上个股(</font><font class="font29">{countStock4}个</font><font class="font4">)：</font><font class="font29">{stock4}</font><font class="font4">,近30个交易日高度板: </font><font class="font29">{self.gaoDuBan} </font><font class="font4">板'''
        return line2

    def _formatLine3(self,lastRow):
        dongNeng = lastRow["动能EX"]
        shiNeng = lastRow["势能EX"]
        beizhu = lastRow["备注"]
        if beizhu is None or len(beizhu) == 0:
            beizhu = "无"
        line3 = f'''
        3. 今日势能:</font><font class="font29">{shiNeng}</font><font class="font4">, 动能:</font><font class="font29">{dongNeng}</font><font class="font4">, 备注:</font><font class="font29">{beizhu}</font><font class="font4">
        '''
        return line3

    def _formatLine4(self,lastRow):
        line4 = f'''
        4. 今日热点板块有: </font><font class="font29">{self.reDianBankuai1} 和 {self.reDianBankuai2}</font>
        '''
        return line4

    def Fomatyizi(self):
        sql = f'''SELECT `日期`,`股票代码`,`股票简称`,`涨停原因类别`,`连续涨停天数`,`封单915`,`封单920`,`封单925` FROM stock.yiziban where `日期` = "{self.today}"  order by `连续涨停天数` DESC;'''
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)
        size = df.shape[0] +1
        high = size*20.40

        yizhi = f'''<tr height="20.40" style='height:20.40pt;'>
        <td class="xl74" height="{high}" colspan="2" rowspan="{size}" style='height:{high}pt;border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;' x:str>二.一字板</td>
        <td class="xl76" x:str>日期</td>
        <td class="xl76" x:str>股票代码</td>
        <td class="xl76" x:str>股票简称</td>
        <td class="xl76" x:str>连续涨停天数</td>
        <td class="xl76" x:str>封单915</td>
        <td class="xl76" x:str>封单920</td>
        <td class="xl76" x:str>封单925</td>
        <td class="xl76" colspan="11" style='border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;' x:str>涨停原因类别</td>
        <td colspan="233" style='mso-ignore:colspan;'></td>
        </tr>'''

        for _,row in df.iterrows():
            stockID = row["股票代码"]
            stcokName = row["股票简称"]
            days = row["连续涨停天数"]
            f15 = row["封单915"]
            f20 = row["封单920"]
            f25 = row["封单925"]
            reason = row["涨停原因类别"]

            stringOfRow = f'''
                <tr height="20.40" style='height:20.40pt;'>
                    <td class="xl77" x:num="45107">{self.today}</td>
                    <td class="xl77" x:str>{stockID}</td>
                    <td class="xl77" x:str>{stcokName}</td>
                    <td class="xl77" x:num>{days}</td>
                    <td class="xl77" x:str>{f15}</td>
                    <td class="xl77" x:str>{f20}</td>
                    <td class="xl77" x:str>{f25}</td>
                    <td class="xl77" colspan="11" style='border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;' x:str>{reason}</td>
                    <td colspan="233" style='mso-ignore:colspan;'></td>
                </tr>
            '''
            yizhi = yizhi + stringOfRow

        yizhi = yizhi + '''
        <tr height="16.80" style='height:16.80pt;'>
            <td class="xl82" height="16.80" colspan="20" style='height:16.80pt;border-right:.5pt solid windowtext;border-bottom:none;'></td>
            <td colspan="233" style='mso-ignore:colspan;'></td>
        </tr>
        '''
        return yizhi

    def FormatMarkingData(self):
        df = self.markingDatas.tail(5)
        df.reset_index(inplace=True)
        # print(df)
        size = df.shape[0] +1
        high = size*20.40
        markingData = f'''
        <tr height="20.40" style='height:20.40pt;'>
            <td class="xl74" height="{high}" colspan="2" rowspan="{size}" style='height:{high}pt;border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;' x:str>三.市场总体情况</td>
            <td class="xl76" x:str>日期</td>
            <td class="xl76" x:str>红盘</td>
            <td class="xl76" x:str>绿盘</td>
            <td class="xl76" x:str>两市量</td>
            <td class="xl76" x:str>量比</td>
            <td class="xl76" x:str>量增</td>
            <td class="xl76" x:str>实际涨停</td>
            <td class="xl76" x:str>跌停</td>
            <td class="xl76" x:str>炸板</td>
            <td class="xl76" x:str>炸板率</td>
            <td class="xl76" x:str>连板个数</td>
            <td colspan="7" style='mso-ignore:colspan;colspan;border-right:.5pt solid windowtext;'></td>
            <td colspan="233" style='mso-ignore:colspan;'></td>
        </tr>'''
        for index,row in df.iterrows():
            style = 'xl77'
            if index == 4:
                style = 'xl78'
            date = row["日期"]
            hongpan = row["红盘"]
            lvpan = row["绿盘"]
            v = row["两市量"]
            vRatio = row["量比"]
            vDelta = row["增量"]
            zhangTing = row["实际涨停"]
            deiting = row["跌停"]
            zhaban = row["炸板"]
            zhabanRatio = row["炸板率"]
            lianbanCount = row["连板"]
            data = f'''
            <tr height="20.40" style='height:20.40pt;'>
            <td class="{style}" x:num="45084">{date}</td>
            <td class="{style}" x:num>{hongpan}</td>
            <td class="{style}" x:num>{lvpan}</td>
            <td class="{style}" x:str>{v}</td>
            <td class="{style}" x:num="-0.13109999999999999">{vRatio}</td>
            <td class="{style}" x:str>{vDelta}</td>
            <td class="{style}" x:num>{zhangTing}</td>
            <td class="{style}" x:num>{deiting}</td>
            <td class="{style}" x:num>{zhaban}</td>
            <td class="{style}" x:num="0.191">{zhabanRatio}</td>
            <td class="{style}" x:num>{lianbanCount}</td>
            <td colspan="7" style='mso-ignore:colspan;border-right:.5pt solid windowtext;'></td>
            <td colspan="233" style='mso-ignore:colspan;'></td>
            </tr>
            '''
            markingData = markingData + data
        markingData = markingData + f'''
                <tr height="16.80" style='height:16.80pt;'>
                    <td class="xl89" height="16.80" colspan="20" style='height:16.80pt;border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;'></td>
                    <td colspan="233" style='mso-ignore:colspan;'></td>
                </tr>
            '''
        return markingData


    def ForamtChaoDuan1(self):
        df = self.markingDatas.tail(10)
        df.reset_index(inplace=True)
        size = df.shape[0] +1
        high = size*20.40
        chaoDuan1 = f'''
        <tr height="20.40" style='height:20.40pt;'>
            <td class="xl67" height="{high}" colspan="2" rowspan="{size}" style='height:{high}.00pt;border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;' x:str>四.超短环境1</td>
            <td class="xl76" x:str>日期</td>
            <td class="xl76" x:str>高度板</td>
            <td class="xl76" x:str>10CM首板奖励率%</td>
            <td class="xl76" x:str>20CM首板奖励率%</td>
            <td class="xl76" x:str>10CM连板奖励率%</td>
            <td class="xl76" x:str>20CM连板奖励率%</td>
            <td class="xl76" x:str>首板个数</td>
            <td class="xl76" x:str>2连板个数</td>
            <td class="xl76" x:str>3连板个数</td>
            <td class="xl76" colspan="2" style='border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;' x:str>3连个股</td>
            <td class="xl76" x:str>4连板及以上个数</td>
            <td class="xl76" colspan="6" style='border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;' x:str>4连及以上个股</td>
        </tr>'''
        for index,row in df.iterrows():
            style = 'xl77'
            if index == size-2:
                style = 'xl78'
            date = row["日期"]
            ratio10 = row["10CM首板奖励率"]
            ratio20 = row["20CM首板奖励率"]
            if ratio20 == "-200.00":
                ratio20 = "-"
            ratio10_l = row["10CM连板奖励率"]
            ratio20_l = row["20CM连板奖励率"]
            if ratio20_l == "-200.00":
                ratio20_l = "-"
            shoubanCount = row["首板个数"]
            lianban2Count = row["2连板个数"]
            lianban3Count = row["3连板个数"]
            lianban3 = row["3连个股"]
            lianban4Count = row["4连板及以上个数"]
            lianban4 = row["4连及以上个股"]
            gaodu = row["高度板"]
            data = f'''
            <tr height="20.40" style='height:20.40pt;'>
                <td class="{style}" x:num="45079">{date}</td>
                <td class="{style}" x:num>{gaodu}</td>
                <td class="{style}" x:num>{ratio10}</td>
                <td class="{style}" x:num>{ratio20}</td>
                <td class="{style}" x:num>{ratio10_l}</td>
                <td class="{style}" x:num>{ratio20_l}</td>
                <td class="{style}" x:num>{shoubanCount}</td>
                <td class="{style}" x:num>{lianban2Count}</td>
                <td class="{style}" x:num>{lianban3Count}</td>
                <td class="{style}" colspan="2" style='border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;' x:str>{lianban3}</td>
                <td class="{style}" x:num>{lianban4Count}</td>
                <td class="{style}" colspan="6" style='border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;' x:str>{lianban4}</td>
            </tr>
            '''
            chaoDuan1 = chaoDuan1 + data
        chaoDuan1 = chaoDuan1 + f'''
                <tr height="16.80" style='height:16.80pt;'>
                    <td class="xl89" height="16.80" colspan="20" style='height:16.80pt;border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;'></td>
                    <td colspan="233" style='mso-ignore:colspan;'></td>
                </tr>
            '''
        return chaoDuan1
    

    def ForamtChaoduan2(self):
        df = self.markingDatas.tail(5)
        df.reset_index(inplace=True)
        size = df.shape[0] +1
        high = size*20.40
        chaoduan2 = f'''
            <tr height="20.40" style='height:20.40pt;'>
                <td class="xl67" height="{high}" colspan="2" rowspan="{size}" style='height:{high}pt;border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;' x:str>五.超短环境2</td>
                <td class="xl76" x:str>日期</td>
                <td class="xl76" x:str>首板率</td>
                <td class="xl76" x:str>连板率</td>
                <td class="xl76" x:str>昨日首板溢价率</td>
                <td class="xl76" x:str>昨日首板晋级率</td>
                <td class="xl76" x:str>昨日2板溢价率</td>
                <td class="xl76" x:str>昨日2板晋级率</td>
                <td class="xl76" x:str>昨日3板溢价率</td>
                <td class="xl76" x:str>昨日3板晋级率</td>
                <td class="xl76" x:str>昨日4板及以上溢价率</td>
                <td class="xl76" x:str>昨日4板及以上晋级率</td>
                <td colspan="7" style='mso-ignore:colspan;border-right:.5pt solid windowtext;'></td>
                <td colspan="233" style='mso-ignore:colspan;'></td>
            </tr>
        '''
        for index,row in df.iterrows():
            style = 'xl77'
            if index == 4:
                style = 'xl78'
            date = row["日期"]
            shoubanRatio = row["首板率"]
            lianbanRatio = row["连板率"]
            shoubanYiJiaRatio = row["昨日首板溢价率"]
            shoubanJinJiRatio = row["昨日首板晋级率"]
            ban2YijiaRatio = row["昨日2板溢价率"]
            ban2JinjiRatio = row["昨日2板晋级率"]
            ban3YijiaRatio = row["昨日3板溢价率"]
            ban3JinjiRatio = row["昨日3板晋级率"]
            ban4YijiaRatio = row["昨日4板及以上溢价率"]
            ban4JinjiRatio = row["昨日4板及以上晋级率"]
            data = f'''
                <tr height="20.40" style='height:20.40pt;'>
                    <td class="{style}" x:num="45079">{date}</td>
                    <td class="{style}" x:num>{shoubanRatio}</td>
                    <td class="{style}" x:num>{lianbanRatio}</td>
                    <td class="{style}" x:num>{shoubanYiJiaRatio}</td>
                    <td class="{style}" x:num>{shoubanJinJiRatio}</td>
                    <td class="{style}" x:num>{ban2YijiaRatio}</td>
                    <td class="{style}" x:num>{ban2JinjiRatio}</td>
                    <td class="{style}" x:num>{ban3YijiaRatio}</td>
                    <td class="{style}" x:num>{ban3JinjiRatio}</td>
                    <td class="{style}" x:num>{ban4YijiaRatio}</td>
                    <td class="{style}" x:num>{ban4JinjiRatio}</td>
                    <td colspan="7" style='mso-ignore:colspan;border-right:.5pt solid windowtext;'></td>
                    <td colspan="233" style='mso-ignore:colspan;'></td>
                </tr>
            '''
            chaoduan2 = chaoduan2 + data
        chaoduan2 = chaoduan2 + f'''
                <tr height="16.80" style='height:16.80pt;'>
                    <td class="xl89" height="16.80" colspan="20" style='height:16.80pt;border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;'></td>
                    <td colspan="233" style='mso-ignore:colspan;'></td>
                </tr>
            '''
        return chaoduan2
    
    def FormatQingXu1(self):
        df = self.markingDatas.tail(5)
        df.reset_index(inplace=True)
        size = df.shape[0] +1
        high = size*20.40
        qingxu1 = f'''
            <tr height="20.40" style='height:20.40pt;'>
                <td class="xl67" height="{high}" colspan="2" rowspan="{size}" style='height:{high}pt;border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;' x:str>六.超短情绪指标1</td>
                <td class="xl76" x:str>日期</td>
                <td class="xl76" x:str>涨停数量</td>
                <td class="xl76" x:str>连板数量</td>
                <td class="xl76" x:str>收-5数量(<100)</td>
                <td class="xl76" x:str>大盘红盘比</td>
                <td class="xl76" x:str>亏钱效应</td>
                <td class="xl76" x:str>首板红盘比(>0.6)</td>
                <td class="xl76" x:str>首板大面比(<0.3)</td>
                <td class="xl76" x:str>连板股的红盘比</td>
                <td class="xl76" x:str>连板比例(>0.6)</td>
                <td class="xl76" x:str>连板大面比(<0.3)</td>
                <td class="xl76" x:str>连板未涨停绿盘比(<0.5)</td>
                <td class="xl76" x:str>势能EX</td>
                <td class="xl76" x:str>动能EX</td>
                <td colspan="4" style='mso-ignore:colspan;border-right:.5pt solid windowtext;'></td>
                <td colspan="233" style='mso-ignore:colspan;'></td>
            </tr>
        '''
        for index,row in df.iterrows():
            style = 'xl77'
            if index == 4:
                style = 'xl78'
            date = row["日期"]

            b = row["涨停数量"]
            c = row["连板数量"]
            d = row["收-5数量"]
            e = row["大盘红盘比"]
            f = row["亏钱效应"]
            g = row["首板红盘比"]
            h = row["首板大面比"]
            i = row["连板股的红盘比"]
            j = row["连板比例"]
            k = row["连板大面比"]
            l = row["昨日连板未涨停数的绿盘比"]
            shiNeng = row["势能EX"]
            dongNeng = row["动能EX"]
            data = f'''
                <tr height="20.40" style='height:20.40pt;'>
                    <td class="{style}" x:num="45079">{date}</td>
                    <td class="{style}" x:num>{b}</td>
                    <td class="{style}" x:num>{c}</td>
                    <td class="{style}" x:num>{d}</td>
                    <td class="{style}" x:num>{e}</td>
                    <td class="{style}" x:num>{f}</td>
                    <td class="{style}" x:num>{g}</td>
                    <td class="{style}" x:num>{h}</td>
                    <td class="{style}" x:num>{i}</td>
                    <td class="{style}" x:num>{j}</td>
                    <td class="{style}" x:num>{k}</td>
                    <td class="{style}" x:num>{l}</td>
                    <td class="{style}" x:num>{shiNeng}</td>
                    <td class="{style}" x:num>{dongNeng}</td>
                    <td colspan="4" style='mso-ignore:colspan;border-right:.5pt solid windowtext;'></td>
                    <td colspan="233" style='mso-ignore:colspan;'></td>
                </tr>
            '''
            qingxu1 = qingxu1 + data
        qingxu1 = qingxu1 + f'''
                <tr height="16.80" style='height:16.80pt;'>
                    <td class="xl89" height="16.80" colspan="20" style='height:16.80pt;border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;'></td>
                    <td colspan="233" style='mso-ignore:colspan;'></td>
                </tr>
            '''
        return qingxu1

    def FormatQingXu2(self):
        df = self.markingDatas.tail(5)
        df.reset_index(inplace=True)
        size = df.shape[0] +1
        high = size*20.40
        qingxu2 = f'''
            <tr height="20.40" style='height:20.40pt;'>
                <td class="xl67" height="{high}" colspan="2" rowspan="{size}" style='height:{high}pt;border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;' x:str>七.超短情绪指标2</td>
                <td class="xl76" x:str>日期</td>
                <td class="xl76" x:str>红盘</td>
                <td class="xl76" x:str>绿盘</td>
                <td class="xl76" x:str>两市量</td>
                <td class="xl76" x:str>实际涨停(>40)</td>
                <td class="xl76" x:str>跌停</td>
                <td class="xl76" x:str>炸板</td>
                <td class="xl76" x:str>炸板率</td>
                <td class="xl76" x:str>连板(>10)</td>
                <td class="xl76" x:str>高度板</td>
                <td class="xl76" x:str>大盘红盘比(>0.4)</td>
                <td class="xl76" x:str>首板红盘比(>0.6)</td>
                <td class="xl76" x:str>连板股的红盘比</td>
                <td class="xl76" x:str>势能EX</td>
                <td class="xl76" x:str>动能EX</td>
                <td class="xl76" x:str>备注</td>
                <td colspan="2" style='mso-ignore:colspan;border-right:.5pt solid windowtext;'></td>
                <td colspan="233" style='mso-ignore:colspan;'></td>
            </tr>
        '''
        for index,row in df.iterrows():
            style = 'xl77'
            if index == 4:
                style = 'xl78'
            date = row["日期"]
            b = row["红盘"]
            c = row["绿盘"]
            d = row["两市量"]
            e = row["实际涨停"]
            f = row["跌停"]
            g = row["炸板"]
            h = row["炸板率"]
            i = row["连板"]
            j = row["高度板"]
            k = row["大盘红盘比"]
            l = row["首板红盘比"]
            m = row["连板股的红盘比"]
            shiNeng = row["势能EX"]
            dongNeng = row["动能EX"]
            beizhu = row["备注"]
            data = f'''
                <tr height="20.40" style='height:20.40pt;'>
                    <td class="{style}" x:num="45079">{date}</td>
                    <td class="{style}" x:num>{b}</td>
                    <td class="{style}" x:num>{c}</td>
                    <td class="{style}" x:num>{d}</td>
                    <td class="{style}" x:num>{e}</td>
                    <td class="{style}" x:num>{f}</td>
                    <td class="{style}" x:num>{g}</td>
                    <td class="{style}" x:num>{h}</td>
                    <td class="{style}" x:num>{i}</td>
                    <td class="{style}" x:num>{j}</td>
                    <td class="{style}" x:num>{k}</td>
                    <td class="{style}" x:num>{l}</td>
                    <td class="{style}" x:num>{m}</td>
                    <td class="{style}" x:num>{shiNeng}</td>
                    <td class="{style}" x:num>{dongNeng}</td>
                    <td class="{style}" x:num>{beizhu}</td>
                    <td colspan="2" style='mso-ignore:colspan;border-right:.5pt solid windowtext;'></td>
                    <td colspan="233" style='mso-ignore:colspan;'></td>
                </tr>
            '''
            qingxu2 = qingxu2 + data
        qingxu2 = qingxu2 + f'''
                <tr height="16.80" style='height:16.80pt;'>
                    <td class="xl89" height="16.80" colspan="20" style='height:16.80pt;border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;'></td>
                    <td colspan="233" style='mso-ignore:colspan;'></td>
                </tr>
            '''
        return qingxu2

    def _SplitLine(self):
        return '''<tr height="16.80" style='height:16.80pt;'>
        <td class="xl99" height="16.80" colspan="20" style='height:16.80pt;border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;'></td>
        <td colspan="233" style='mso-ignore:colspan;'></td>
    </tr>'''

    def _formatRedian(self,bankuai,df):
        size = df.shape[0] +1
        high = size*20.40
        redian = f'''
    <tr height="20.40" style='height:20.40pt;'>
        <td class="xl68" height="{high:.2f}" colspan="2" rowspan="{size}" style='height:{high:.2f}pt;border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;' x:str>{bankuai}</font></td>
        <td class="xl76" x:str>日期</td>
        <td class="xl76" x:str>股票代码</td>
        <td class="xl76" x:str>股票简称</td>
        <td class="xl76" x:str>连续涨停天数</td>
        <td class="xl76" x:str>首次涨停时间</td>
        <td class="xl76" x:str>最终涨停时间</td>
        <td class="xl76" x:str>板数</td>
        <td class="xl76" x:str>成交额</td>
        <td class="xl76" x:str>成交额4%</td>
        <td class="xl76" x:str>成交额8%</td>
        <td class="xl76" x:str>成交额10%</td>
        <td class="xl76" x:str>流通市值</td>
        <td class="xl76" colspan="6" style='border-right:none;border-bottom:none;' x:str>涨停原因类别</td>
        <td colspan="233" style='mso-ignore:colspan;'></td>
    </tr>'''
        for index,row in df.iterrows():
            date = row["日期"]
            stockID = row["股票代码"]
            stcokName = row["股票简称"]
            days = row["连续涨停天数"]
            time1 = row["首次涨停时间"]
            time2 = row["最终涨停时间"]
            banshu = row["涨停关键词"]
            e = row["新成交额"]
            e4 = row["成交额4%"]
            e8 = row["成交额8%"]
            e10 = row["成交额10%"]
            v = row["流通市值"]
            reason = row["涨停原因类别"]
            if index % 2 == 0:
                style1 = "xl102"
                style2 = "xl106"
                if re.match('^30.*',stockID) is not None:
                    style1 = "xl104"
                    style2 = "xl108"
                stringOfRow = f'''
                    <tr height="20.40" style='height:20.40pt;'>
                        <td class="{style1}" x:num="45084">{date}</td>
                        <td class="{style1}" x:str>{stockID}</td>
                        <td class="{style1}" x:str>{stcokName}</td>
                        <td class="{style1}" x:num>{days}</td>
                        <td class="{style2}" x:num="0.39618055555555554">{time1}</td>
                        <td class="{style2}" x:num="0.39618055555555554">{time2}</td>
                        <td class="{style1}" x:str>{banshu}</td>
                        <td class="{style1}" x:str>{e}</td>
                        <td class="{style1}" x:str>{e4}</td>
                        <td class="{style1}" x:str>{e8}</td>
                        <td class="{style1}" x:str>{e10}</td>
                        <td class="{style1}" x:str>{v}</td>
                        <td class="{style1}" colspan="6" style='border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;' x:str>{reason}</td>
                        <td colspan="233" style='mso-ignore:colspan;'></td>
                    </tr>
                '''
            else:
                style1 = "xl101"
                style2 = "xl105"
                if re.match('^30.*',stockID) is not None:
                    style1 = "xl103"
                    style2 = "xl107"
                stringOfRow = f'''
                    <tr height="20.40" style='height:20.40pt;'>
                        <td class="{style1}" x:num="45084">{date}</td>
                        <td class="{style1}" x:str>{stockID}</td>
                        <td class="{style1}" x:str>{stcokName}</td>
                        <td class="{style1}" x:num>{days}</td>
                        <td class="{style2}" x:num="0.39618055555555554">{time1}</td>
                        <td class="{style2}" x:num="0.39618055555555554">{time2}</td>
                        <td class="{style1}" x:str>{banshu}</td>
                        <td class="{style1}" x:str>{e}</td>
                        <td class="{style1}" x:str>{e4}</td>
                        <td class="{style1}" x:str>{e8}</td>
                        <td class="{style1}" x:str>{e10}</td>
                        <td class="{style1}" x:str>{v}</td>
                        <td class="{style1}" colspan="6" style='border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;' x:str>{reason}</td>
                        <td colspan="233" style='mso-ignore:colspan;'></td>
                    </tr>
                '''
            redian = redian + stringOfRow
        redian = redian + '''<tr height="16.80" style='height:16.80pt;'>
        <td class="xl99" height="16.80" colspan="20" style='height:16.80pt;border-right:none;border-bottom:none;'></td>
        <td colspan="233" style='mso-ignore:colspan;'></td>
        </tr>'''
        return redian
    
    def FormatReDian1(self):
        title = f'''八.热点1</br><font color="red">[{self.reDianBankuai1}]</font>'''
        redian1 = self._formatRedian(title,self.reDianBankuai1DF)
        return redian1

    def FormatReDian2(self):
        title = f'''九.热点2</br><font color="red">[{self.reDianBankuai2}]</font>'''
        redian2 = self._formatRedian(title,self.reDianBankuai2DF)
        return redian2

    def ForamtLianBanGu(self):
        title = f'''十.连板股'''
        lianban = self._formatRedian(title,self.lianbanDF)
        return lianban
     
    def FormatShouBanGu(self):
        title = f'''十一.首板股'''
        shouban = self._formatRedian(title,self.shoubanDF)
        return shouban

    def ForamtTail(self):
        tail = '''
        <![if supportMisalignedColumns]>
            <tr width="0" style='display:none;'>
            <td width="48" style='width:48;'></td>
            <td width="110" style='width:110;'></td>
            <td width="82" style='width:82;'></td>
            <td width="113" style='width:113;'></td>
            <td width="114" style='width:114;'></td>
            <td width="121" style='width:121;'></td>
            <td width="124" style='width:124;'></td>
            <td width="107" style='width:107;'></td>
            <td width="125" style='width:125;'></td>
            <td width="105" style='width:105;'></td>
            <td width="133" style='width:133;'></td>
            <td width="141" style='width:141;'></td>
            <td width="152" style='width:152;'></td>
            <td width="157" style='width:157;'></td>
            <td width="113" style='width:113;'></td>
            <td width="68" style='width:68;'></td>
            <td width="57" style='width:57;'></td>
            <td width="75" style='width:75;'></td>
            <td width="48" style='width:48;'></td>
            </tr>
        <![endif]>
        </table>
        </body>
        </html>
        '''
        return tail


    def WirteFupanSummary(self):
        self.markingDatas = self._QueryMarkingData()
        #print(markingDatas)
        self.markingDatas['连板'] = self.markingDatas['连板'].astype(int)
        self.markingDatas['高度板'] = self.markingDatas['高度板'].astype(int)
        lastRow = self.markingDatas.iloc[-1]
        self.maxLianBanCount = max(self.markingDatas['连板'])
        self.minLianBanCount = min(self.markingDatas['连板'])
        self.gaoDuBan = max(self.markingDatas['高度板'])
        self._AnalysisZhangTingReason()
        head = self.FormatHead()
        title = self.FormatTitle()
        warning = self.FormatWarning()
        line1 = self._formatLine1(lastRow)
        line2 = self._formatLine2(lastRow)
        line3 = self._formatLine3(lastRow)
        line4 = self._formatLine4(lastRow)

        summaryOfToday = self.FormateSummaryOfToday(line1,line2,line3,line4)
        yizi = self.Fomatyizi()
        marking = self.FormatMarkingData()
        chaoduan1 = self.ForamtChaoDuan1()
        chaoduan2 = self.ForamtChaoduan2()
        qingxu1 = self.FormatQingXu1()
        qingxu2 = self.FormatQingXu2()
        redian1 = self.FormatReDian1()
        redian2 = self.FormatReDian2()
        lianban = self.ForamtLianBanGu()
        shouban = self.FormatShouBanGu()
        tail = self.ForamtTail()

        Summary = f'''{head}{title}{warning}{summaryOfToday}{yizi}{marking}{chaoduan1}{chaoduan2}{qingxu1}{qingxu2}{redian1}{redian2}{lianban}{shouban}{tail}'''    
        fileName = f'''{workSpaceRoot}/复盘/股票/{self.today}/复盘摘要_{self.today}.htm'''
        with open(fileName,"w+") as f:
            f.write(Summary)
            print("写入摘要:" + fileName + "  成功！！")

        sqlSummary = Summary.replace('  ',' ').replace('\n','').replace("'","\\'").replace('"','\"')
        sql = f'''REPLACE INTO `stock`.`fupansummary` (`date`, `summary`) VALUES ('{self.today}', '{sqlSummary}');'''

        self.dbConnection.Execute(sql)

        

    def GetFuPanLastSummary(self):
        sql = f'''SELECT summary FROM stock.fupansummary order by date DESC limit 1;'''
        results, _ = self.dbConnection.Query(sql)
        summary = results[0][0]
        path = "/home/jenkins/Summary/"
        import os
        if os.path.exists(path) == False:
            os.makedirs(path)
        with open(f"{path}summary.html",'w+') as f:
            f.write(summary)