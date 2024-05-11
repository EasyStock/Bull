
import re

def __formatReason(reason):
    reasons = reason.split(('['))
    return "  [".join(reasons)

def _fornatZhuanZaiReason(reasons):
    reasons = reasons.replace("\t"," ").replace("\u3000"," ").replace("\n","").split(";")
    if len(reasons) == 1:
        return __formatReason(reasons[0])
    else:
        reason = "\n".join([__formatReason(t.strip()) for t in reasons])
        return "\n"+reason

def FormatCardOfZhuanZaiYuJing(date,df):
    #转债预警卡片消息
    if df.empty:
        return None
    contents = []
    tag = {"tag":"hr"}
    #alarmPattern = "[\s\S]*(平均市净率:|有息负债率:|流通市值:|评   级:|剩余年限:)+?[\s\S]*"
    alarmPattern = "[\s\S]*(有息负债率:|评   级:|无强赎公告|剩余规模|ST)+?[\s\S]*"
    result = False
    for _, row in df.iterrows():
        reasons = _fornatZhuanZaiReason(row["原因"])
        stockID = row["转债代码"]
        stockName = row["转债名称"]
        reason = f'''**<font color=red>{reasons}</font>**'''
        if re.match(alarmPattern,reasons) == None:
            continue
        s = f"**转债名称** : {stockName}\n**转债代码** : {stockID}\n**原        因** : {reason}"
        content = {"content":s,"tag":"markdown"}
        contents.append(content)
        contents.append(tag)
        result = True
    if result == False:
        return None
    t = f"预警与可能的机会提醒:{date}"
    title = {"content":t,"tag":"plain_text"}
    beizhu = {"elements":[{"content":"说明:当出现以下几种情况会出现本条消息:\n1. 有息负债率大于70了\n2. 评级不符了\n3. 公告要强赎 或 临近到期\n4. 剩余规模<3.5\n5. 正股被ST了\n\n风险提示: 本内容仅信息分享,不构成投资建议,若以此作为买卖依据,后果自负。市场有风险,投资需谨慎！","tag":"plain_text"}],"tag":"note"}
    contents.append(beizhu)
    ret = {"config":{"wide_screen_mode":True},"elements":contents, "header":{"template":"red","title":title}}
    return ret

def FormatCardOfNewGaiNian(date,gainian, stocks,titles,showDetails = False):
    elements = []
    tag = {"tag":"hr"}
    stockHead = {"tag":"column_set","flex_mode":"none","background_style":"grey","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[0],"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[1],"text_align":"center"}]}]}
    elements.append(tag)
    elements.append(stockHead)

    if showDetails:
        for stock in stocks:
            stockID = f'''{stock[0]}'''
            stockName = f'''{stock[1]}'''
            line = {"tag":"column_set","flex_mode":"none","background_style":"default","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockID,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockName,"text_align":"center"}]}],"horizontal_spacing":"small"}
            elements.append(line)

    elements.append(tag)
    beizhu = {"elements":[{"content":"风险提示: 本内容仅信息分享,不构成投资建议,若以此作为买卖依据,后果自负。市场有风险,投资需谨慎！","tag":"plain_text"}],"tag":"note"}
    elements.append(beizhu)
    title = f"{date} 新增概念:{gainian}"
    return {"config":{"wide_screen_mode":True},"elements":elements,"header":{"template":"turquoise","title":{"content":title,"tag":"plain_text"}}}

def FormatCardOfReDianToday(date,redian,stocks,titles):
    elements = []
    tag = {"tag":"hr"}
    stockHead = {"tag":"column_set","flex_mode":"none","background_style":"grey","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[0],"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[1],"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[2],"text_align":"center"}]}]}
    elements.append(tag)
    elements.append(stockHead)

    background_style = "default"
    for index,stock in enumerate(stocks):
        if index % 2 == 1:
            background_style = "grey"
        stockID = stock[0]
        stockName = f'''{stock[1][0]}'''
        r = f'''{stock[1][1]}'''
        line = {"tag":"column_set","flex_mode":"none","background_style":background_style,"columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockID,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockName,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":r,"text_align":"center"}]}],"horizontal_spacing":"small"}
        elements.append(line)

    elements.append(tag)
    beizhu = {"elements":[{"content":"风险提示: 本内容仅信息分享,不构成投资建议,若以此作为买卖依据,后果自负。市场有风险,投资需谨慎！","tag":"plain_text"}],"tag":"note"}
    elements.append(beizhu)
    title = f"{date} 今日炒作热点: {redian},有此概念的可转债如下:"
    return {"config":{"wide_screen_mode":True},"elements":elements,"header":{"template":"red","title":{"content":title,"tag":"plain_text"}}}

def FormatCardOfNewGaiNian5Days(data,title):
    tag = {"tag":"hr"}
    elements = []
    for index,d in enumerate(data):
        date = d[0]
        gaiNian = d[1]
        stocks = d[2]
        if len(stocks) == 0:
            s = f'''{index+1}. {date}  新增概念:**{_markdownFontColor(gaiNian)}**,**但无此概念相关的可转债**'''
            line1 = {"content":s,"tag":"markdown"}
            elements.append(tag)
            elements.append(line1)
        else:
            s = f'''{index+1}. {date}  新增概念:**{_markdownFontColor(gaiNian)}**,**此概念相关的可转债如下:**'''
            line1 = {"content":s,"tag":"markdown"}
            stockHead = {"tag":"column_set","flex_mode":"none","background_style":"grey","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":"转债代码","text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":"转债名称","text_align":"center"}]}]}
            elements.append(line1)
            elements.append(stockHead)
            for s in stocks:
                stockID = s[0]
                stockName = s[1]
                line2 = {"tag":"column_set","flex_mode":"none","background_style":"default","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockID,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockName,"text_align":"center"}]}],"horizontal_spacing":"small"}
                elements.append(line2)
                
        elements.append(tag)

    beizhu = {"elements":[{"content":"风险提示: 本内容仅信息分享,不构成投资建议,若以此作为买卖依据,后果自负。市场有风险,投资需谨慎！","tag":"plain_text"}],"tag":"note"}
    elements.append(beizhu)
    return {"config":{"wide_screen_mode":True},"elements":elements,"header":{"template":"orange","title":{"content":title,"tag":"plain_text"}}}

def _markdownFontColor(data,color = "red"):
    return f'''**<font color='{color}'>{data}</font>**'''

def _markdownFontHighlight(data):
    return f'''**{data}**'''

def _markdown(data):
    tag = {"tag":"markdown","content":data}
    return tag


def _fomatItem(df,lable,key,color = "red",showYesterday = True):
    if showYesterday:
        return f'''{lable}:{_markdownFontColor(df[key].iloc[-1],color)}(昨日:{_markdownFontColor(df[key].iloc[-2],color)})'''
    else:
        return f'''{lable}:{_markdownFontColor(df[key].iloc[-1],color)}'''

def FormatCardOfMeiRiFuPan(date,df,redian,redianDfs):
    elements = []
    tag = {"tag":"hr"}
    avgLianBanCount = f'''{df["平均连板数量"][0]:.1f}'''

    line1 = f'''{_markdownFontHighlight("一. 市场总体情况:")}\n**1.** {_fomatItem(df,"今成交量","两市量")},{_fomatItem(df,"增量","增量",showYesterday=False)}\n**2.** {_fomatItem(df,"红盘","红盘")}, {_fomatItem(df,"绿盘","绿盘","green")}\n**3.** {_fomatItem(df,"涨停","实际涨停")}, {_fomatItem(df,"跌停","跌停","green")}\n**4.** {_fomatItem(df,"炸板","炸板","green")}，{_fomatItem(df,"炸板率","炸板率","red")}\n**5.** {_fomatItem(df,"连板个数","连板数量","red")},近30个交易日最多连板个数: {_markdownFontColor(df["最高连板数量"].iloc[-1])},最少连板个数: {_markdownFontColor(df["最低连板数量"].iloc[-1])},平均连板个数: {_markdownFontColor(avgLianBanCount)}'''
    elements.append(_markdown(line1))
    elements.append(tag)

    line2 = f'''{_markdownFontHighlight("二. 市场情绪:")}\n{_fomatItem(df,"今日势能","势能EX")},{_fomatItem(df,"动能","动能EX")}, {_fomatItem(df,"连板股的红盘比","连板股的红盘比")}, {_fomatItem(df,"首板红盘比","首板红盘比")}, 备注:{_markdownFontColor(df["备注"].iloc[-1])}\n\n**情绪判断标准**\n**1. 高潮:** {_markdownFontColor("动能综合值=12 且 势能综合值=10 或者 连板股的红盘比 >=0.78 首板股的红盘比 >=0.78","green")}\n**2. 半高潮:** {_markdownFontColor("只有 连板股的红盘比 >=0.78","green")}\n**3. 冰点期判断 - 强势行情:** {_markdownFontColor("如果动能综合值 =-12 且 势能综合值 <=-2 或者 (动能综合值<=-8 且 势能综合值<=-2) 出现两次","green")}\n**4. 冰点期判断 - 弱势行情:** {_markdownFontColor("如果动能综合值 <=-8 且 势能综合值 =-10 且首板赚钱效应和连板赚钱效应都出现过 <0.4 或者 连续两天动能综合值和势能综合值都<=-6","green")}'''
    elements.append(_markdown(line2))
    elements.append(tag)

    pojudian = ""
    if int(df["2连板个数"].iloc[-1]) >=9:
        pojudian = _markdownFontColor("(今天可能是破局点)")
    
    line3 = f'''{_markdownFontHighlight("三. 超短情况:")}\n**1.**10CM首板奖励率:{_markdownFontColor(df["10CM首板奖励率"].iloc[-1])}%(昨日:{_markdownFontColor(df["10CM首板奖励率"].iloc[-2])}%),10CM连板奖励率:{_markdownFontColor(df["10CM连板奖励率"].iloc[-1])}%(昨日:{_markdownFontColor(df["10CM连板奖励率"].iloc[-2])})\n**2.**20CM首板奖励率:{_markdownFontColor(df["20CM首板奖励率"].iloc[-1])}%(昨日:{_markdownFontColor(df["20CM首板奖励率"].iloc[-2])}%),20CM连板奖励率:{_markdownFontColor(df["20CM连板奖励率"].iloc[-1])}%(昨日:{_markdownFontColor(df["20CM连板奖励率"].iloc[-2])}%)\n**3.**{_fomatItem(df,"2连板个股","2连板个数")}{pojudian},近30日最大2连板个股:{_markdownFontColor(df["最大2连板个数"].iloc[-1])}个\n**4.**{_fomatItem(df,"3连板个股","3连板个数")}: {_markdownFontColor(df["3连个股"].iloc[-1])}\n**5.**{_fomatItem(df,"4连及以上个股","4连板及以上个数")},{_markdownFontColor(df["4连及以上个股"].iloc[-1])}\n**6.**近30个交易日高度板: {_markdownFontColor(df["最大高度板"].iloc[-1])} 板'''
    elements.append(_markdown(line3))
    elements.append(tag)

    line4 = f"**四. 今日热点板块:** {_markdownFontColor(redian)}"
    elements.append(_markdown(line4))
    elements.append(tag)

    beizhu = {"elements":[{"content":"风险提示: 本内容仅信息分享,不构成投资建议,若以此作为买卖依据,后果自负。市场有风险,投资需谨慎！","tag":"plain_text"}],"tag":"note"}
    elements.append(beizhu)
    title = f" 每日复盘 - {date}"
    return {"config":{"wide_screen_mode":True},"elements":elements,"header":{"template":"red","title":{"content":title,"tag":"plain_text"}}}



def FormatCardOfAlarm(date,alarm:list):
    elements = []
    tag = {"tag":"hr"}

    line1 = "\n".join(alarm)
    elements.append(_markdown(line1))
    elements.append(tag)

    beizhu = {"elements":[{"content":"风险提示: 本内容仅信息分享,不构成投资建议,若以此作为买卖依据,后果自负。市场有风险,投资需谨慎！","tag":"plain_text"}],"tag":"note"}
    elements.append(beizhu)
    title = f" 每日指数预警 - {date}"
    return {"config":{"wide_screen_mode":True},"elements":elements,"header":{"template":"red","title":{"content":title,"tag":"plain_text"}}}


def FormatCardOfQiangShu(datas,title):
    #强制赎回公告
    if len(datas) == 0:
        return None
    elements = []
    tag = {"tag":"hr"}
    stockHead = {"tag":"column_set","flex_mode":"none","background_style":"grey","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":"**转债名称**","text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":"**公告内容**","text_align":"center"}]}]}
    elements.append(tag)
    elements.append(stockHead)

    for stockID in datas:
        stockName = f'''{datas[stockID]["转债名称"]}'''
        tips = "\n".join(datas[stockID]["公告"])
        price = datas[stockID]["现价"]
        if price <=140:
            stockName = _markdownFontColor(stockName)
            tips = _markdownFontColor(tips)

        line = {"tag":"column_set","flex_mode":"none","background_style":"default","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockName,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":tips,"text_align":"center"}]}],"horizontal_spacing":"small"}
        elements.append(line)

    elements.append(tag)
    beizhu = {"elements":[{"content":"说明:转债现价 <= 140 显示红色\n\n风险提示: 本内容仅信息分享,不构成投资建议,若以此作为买卖依据,后果自负。市场有风险,投资需谨慎！","tag":"plain_text"}],"tag":"note"}
    elements.append(beizhu)
    return {"config":{"wide_screen_mode":True},"elements":elements,"header":{"template":"red","title":{"content":title,"tag":"plain_text"}}}


def FormatCardOfNewStock(datas,title):
    #新股发行
    if len(datas) == 0:
        return None
    elements = []
    tag = {"tag":"hr"}
    stockHead = {"tag":"column_set","flex_mode":"none","background_style":"grey","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":"**名称**","text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":"**日期**","text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":"**类别**","text_align":"center"}]}]}
    elements.append(tag)
    elements.append(stockHead)

    for index,stock in enumerate(datas):
        stockName = stock[0]
        date = stock[1]
        type = stock[2]
        background_style = "default"
        if index % 2 == 1:
            background_style = "grey"

        if  type.find("北交所") != -1:
            stockName = _markdownFontColor(stockName)
            date = _markdownFontColor(date)
            type = _markdownFontColor(type)

        line = {"tag":"column_set","flex_mode":"none","background_style":background_style,"columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockName,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":date,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":type,"text_align":"center"}]}],"horizontal_spacing":"small"}
        elements.append(line)


    elements.append(tag)
    beizhu = {"elements":[{"content":"风险提示: 本内容仅信息分享,不构成投资建议,若以此作为买卖依据,后果自负。市场有风险,投资需谨慎！","tag":"plain_text"}],"tag":"note"}
    elements.append(beizhu)
    return {"config":{"wide_screen_mode":True},"elements":elements,"header":{"template":"red","title":{"content":title,"tag":"plain_text"}}}


def FormatCardOfKeZhuanZaiScore(date,df,titles,diDianDate,limit):
    ###可转债综合分数通知
    elements = []
    tag = {"tag":"hr"}
    s = f'''<font color="red">下跌区间:\n{df.iloc[0]["抗跌分数周期"]}\n\n上涨区间:\n{df.iloc[0]["领涨分数周期"]}\n\n最低时间点:{diDianDate}\n\n</font>'''
    line1 = {"content":s,"tag":"markdown"}
    elements.append(line1)

    stockHead = {"tag":"column_set","flex_mode":"none","background_style":"grey","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[0],"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[1],"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[2],"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[3],"text_align":"center"}]}]}
    elements.append(tag)
    elements.append(stockHead)

    background_style = "default"
    for index, row in df.iterrows():
        if index % 2 == 1:
            background_style = "grey"
        stockID = row["转债代码"]
        stockName = row["转债名称"]
        r = f'''{row["总分"]:.2f}'''
        line = {"tag":"column_set","flex_mode":"none","background_style":background_style,"columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":str(index+1),"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockID,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockName,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":r,"text_align":"center"}]}],"horizontal_spacing":"small"}
        elements.append(line)

    elements.append(tag)
    beizhu = {"elements":[{"content":"风险提示: 本内容仅信息分享,不构成投资建议,若以此作为买卖依据,后果自负。市场有风险,投资需谨慎！","tag":"plain_text"}],"tag":"note"}
    elements.append(beizhu)
    title = f'''{date} 抗跌、反弹、成交量、剩余规模 综合评分 前{limit}个'''
    return {"config":{"wide_screen_mode":True},"elements":elements,"header":{"template":"red","title":{"content":title,"tag":"plain_text"}}}


def FormatCardOfKeZhuanZaiPingJiChanged(date,df,titles):
    ###可转债评级变动通知
    elements = []
    tag = {"tag":"hr"}
    stockHead = {"tag":"column_set","flex_mode":"none","background_style":"grey","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[0],"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[1],"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[2],"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[3],"text_align":"center"}]}]}
    elements.append(tag)
    elements.append(stockHead)

    background_style = "default"
    for index, row in df.iterrows():
        if index % 2 == 1:
            background_style = "grey"
        stockID = row["转债代码"]
        stockName = row["转债名称"]
        pingJi = row["评级"]
        pingJiYestoday = row["昨日评级"]
        line = {"tag":"column_set","flex_mode":"none","background_style":background_style,"columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockID,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockName,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":pingJiYestoday,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":pingJi,"text_align":"center"}]}],"horizontal_spacing":"small"}
        elements.append(line)

    elements.append(tag)
    beizhu = {"elements":[{"content":"风险提示: 本内容仅信息分享,不构成投资建议,若以此作为买卖依据,后果自负。市场有风险,投资需谨慎！","tag":"plain_text"}],"tag":"note"}
    elements.append(beizhu)
    title = f'''{date} 评级变动通知'''
    return {"config":{"wide_screen_mode":True},"elements":elements,"header":{"template":"red","title":{"content":title,"tag":"plain_text"}}}


def ForamtKeZhuanZaiSummary(datas:list,date):
    bigIndex = ["一. ","二. ","三. ","四. ","五. ","六. ","七. ","八. ","九. ","十. ",]
    elements = []
    tag = {"tag":"hr"}
    for index, element in enumerate(datas):
        if len(element) == 0:
            continue
        firstLine = f'''**{bigIndex[index]}** {element[0]}\n'''
        elements.append(_markdown(firstLine))
        if len(element) > 1:
            elements.extend(element[1:])

        elements.append(tag)

    beizhu = {"elements":[{"content":"风险提示: 本内容仅信息分享,不构成投资建议,若以此作为买卖依据,后果自负。市场有风险,投资需谨慎！","tag":"plain_text"}],"tag":"note"}
    elements.append(beizhu)
    title = f'''可转债复盘 {date} '''
    return {"config":{"wide_screen_mode":True},"elements":elements,"header":{"template":"red","title":{"content":title,"tag":"plain_text"}}}


def ForamtKeZhuanZaiSummary_PingJi(df,titles):
    ###可转债评级变动通知
    if df.empty: 
        return []
    
    title = _markdownFontColor("今日可转债评级变动","green")
    elements = [title,]
    stockHead = {"tag":"column_set","flex_mode":"none","background_style":"grey","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[0],"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[1],"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[2],"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[3],"text_align":"center"}]}]}
    elements.append(stockHead)

    background_style = "default"
    for index, row in df.iterrows():
        if index % 2 == 1:
            background_style = "grey"
        stockID = row["转债代码"]
        stockName = row["转债名称"]
        pingJi = _markdownFontColor(row["评级"])
        pingJiYestoday = _markdownFontColor(row["昨日评级"],"green")
        line = {"tag":"column_set","flex_mode":"none","background_style":background_style,"columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockID,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockName,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":pingJiYestoday,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":pingJi,"text_align":"center"}]}],"horizontal_spacing":"small"}
        elements.append(line)

    return elements

def ForamtKeZhuanZaiSummary_PingJiBuFu(df,titles):
    ###可转债评级不符合
    if df.empty: 
        return []
    
    title = _markdownFontColor("近5日可转债评级不符合","green")
    elements = [title,]

    stockHead = {"tag":"column_set","flex_mode":"none","background_style":"grey","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[0],"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[1],"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[2],"text_align":"center"}]},]}
    elements.append(stockHead)

    background_style = "default"
    for index, row in df.iterrows():
        if index % 2 == 1:
            background_style = "grey"
        stockID = row["转债代码"]
        stockName = row["转债名称"]
        pingJi = _markdownFontColor(row["评级"])
        line = {"tag":"column_set","flex_mode":"none","background_style":background_style,"columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockID,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockName,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":pingJi,"text_align":"center"}]}],"horizontal_spacing":"small"}
        elements.append(line)

    return elements

def ForamtKeZhuanZaiSummary_YouXiFuZhai(df,titles):
    ###有息负债率大于70
    if df.empty: 
        return []
    
    title = _markdownFontColor("近5日可转债有息负债率大于70","green")
    elements = [title,]

    stockHead = {"tag":"column_set","flex_mode":"none","background_style":"grey","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[0],"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[1],"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[2],"text_align":"center"}]},]}
    elements.append(stockHead)

    background_style = "default"
    for index, row in df.iterrows():
        if index % 2 == 1:
            background_style = "grey"
        stockID = row["转债代码"]
        stockName = row["转债名称"]
        fuzhailv = _markdownFontColor(row["有息负债率"])
        line = {"tag":"column_set","flex_mode":"none","background_style":background_style,"columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockID,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockName,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":fuzhailv,"text_align":"center"}]}],"horizontal_spacing":"small"}
        elements.append(line)

    return elements

def ForamtKeZhuanZaiSummary_shengyuGuiMo(df,titles):
    ###剩余规模小于3.5
    if df.empty: 
        return []
    
    title = _markdownFontColor("近5日可转债剩余规模小于3.5","green")
    elements = [title,]

    stockHead = {"tag":"column_set","flex_mode":"none","background_style":"grey","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[0],"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[1],"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[2],"text_align":"center"}]},]}
    elements.append(stockHead)

    background_style = "default"
    for index, row in df.iterrows():
        if index % 2 == 1:
            background_style = "grey"
        stockID = row["转债代码"]
        stockName = row["转债名称"]
        fuzhailv = _markdownFontColor(row["剩余规模"])
        line = {"tag":"column_set","flex_mode":"none","background_style":background_style,"columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockID,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockName,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":fuzhailv,"text_align":"center"}]}],"horizontal_spacing":"small"}
        elements.append(line)

    return elements

def ForamtKeZhuanZaiSummary_ST(df,titles):
    ###正股被ST了
    if df.empty: 
        return []
    
    title = _markdownFontColor("近5日可转债正股被ST","green")
    elements = [title,]

    stockHead = {"tag":"column_set","flex_mode":"none","background_style":"grey","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[0],"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[1],"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[2],"text_align":"center"}]},]}
    elements.append(stockHead)

    background_style = "default"
    for index, row in df.iterrows():
        if index % 2 == 1:
            background_style = "grey"
        stockID = row["转债代码"]
        stockName = _markdownFontColor(row["转债名称"])
        fuzhailv = _markdownFontColor(row["正股名称"])
        line = {"tag":"column_set","flex_mode":"none","background_style":background_style,"columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockID,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockName,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":fuzhailv,"text_align":"center"}]}],"horizontal_spacing":"small"}
        elements.append(line)

    return elements

def ForamtKeZhuanZaiSummary_NewStock(datas):
    ###新股申购
    if len(datas) == 0:
        return []
    
    title = _markdownFontColor("新股申购","green")
    elements = [title,]
    stockHead = {"tag":"column_set","flex_mode":"none","background_style":"grey","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":"**名称**","text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":"**日期**","text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":"**类别**","text_align":"center"}]}]}
    elements.append(stockHead)

    for index,stock in enumerate(datas):
        stockName = stock[0]
        date = stock[1]
        type = stock[2]
        background_style = "default"
        if index % 2 == 1:
            background_style = "grey"

        if  type.find("北交所") != -1:
            stockName = _markdownFontColor(stockName)
            date = _markdownFontColor(date)
            type = _markdownFontColor(type)

        line = {"tag":"column_set","flex_mode":"none","background_style":background_style,"columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockName,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":date,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":type,"text_align":"center"}]}],"horizontal_spacing":"small"}
        elements.append(line)

    return elements

def ForamtKeZhuanZaiSummary_qiangsu(datas):
    #强赎公告
    if len(datas) == 0:
        return []
    
    title = _markdownFontColor("近5日临近到期或强赎提醒","green")
    elements = [title,]
    stockHead = {"tag":"column_set","flex_mode":"none","background_style":"grey","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":"**转债名称**","text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":"**公告内容**","text_align":"center"}]}]}
    elements.append(stockHead)

    for stockID in datas:
        stockName = f'''{datas[stockID]["转债名称"]}'''
        tips = "\n".join(datas[stockID]["公告"])
        price = datas[stockID]["现价"]
        if price <=140:
            stockName = _markdownFontColor(stockName)
            tips = _markdownFontColor(tips)

        line = {"tag":"column_set","flex_mode":"none","background_style":"default","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockName,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":tips,"text_align":"center"}]}],"horizontal_spacing":"small"}
        elements.append(line)

    return elements


def ForamtKeZhuanZaiSummary_NewGaiNian5Days(datas):
    if len(datas) == 0:
        return []
    
    title = _markdownFontColor("近5日新增概念","green")
    elements = [title,]
    for index,d in enumerate(datas):
        date = d[0]
        gaiNian = d[1]
        stocks = d[2]
        if len(stocks) == 0:
            s = f'''**{_markdownFontColor(date)}**  新增概念:**{_markdownFontColor(gaiNian)}** **(无此概念相关的可转债)**'''
            line1 = {"content":s,"tag":"markdown"}
            elements.append(line1)
        else:
            s = f'''{date}  新增概念:**{_markdownFontColor(gaiNian)}**,**此概念相关的可转债如下:**'''
            line1 = {"content":s,"tag":"markdown"}
            stockHead = {"tag":"column_set","flex_mode":"none","background_style":"grey","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":"转债代码","text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":"转债名称","text_align":"center"}]}]}
            elements.append(line1)
            elements.append(stockHead)
            for s in stocks:
                stockID = s[0]
                stockName = s[1]
                line2 = {"tag":"column_set","flex_mode":"none","background_style":"default","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockID,"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"center","elements":[{"tag":"markdown","content":stockName,"text_align":"center"}]}],"horizontal_spacing":"small"}
                elements.append(line2)
                
        elements.append({"content":"\n","tag":"markdown"})

    return elements


def ForamtKeZhuanZaiSummary_ReDianToday(datas):
    if len(datas) == 0:
        return []
    
    title = _markdownFontColor("今日热点概念Top2","green")
    elements = [title,]
    for gainian in datas:
        df = datas[gainian]
        stockNames = "       ".join(list(df['转债名称']))
        s = f'''{_markdownFontColor(gainian)} **概念:**\n {stockNames}'''
        line1 = {"content":s,"tag":"markdown"}
        elements.append(line1)

    return elements

