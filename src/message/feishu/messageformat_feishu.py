
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
    for _, row in df.iterrows():
        reasons = _fornatZhuanZaiReason(row["原因"])
        stockID = row["转债代码"]
        stockName = row["转债名称"]
        reason = f'''**<font color=red>{reasons}</font>**'''
        s = f"**转债名称** : {stockName}\n**转债代码** : {stockID}\n**原        因** : {reason}"
        content = {"content":s,"tag":"markdown"}
        contents.append(content)
        contents.append(tag)
    t = f"条件不符合预警:{date}"
    title = {"content":t,"tag":"plain_text"}
    beizhu = {"elements":[{"content":"风险提示: 本内容仅信息分享,不构成投资建议,若以此作为买卖依据,后果自负。市场有风险,投资需谨慎！","tag":"plain_text"}],"tag":"note"}
    contents.append(beizhu)
    ret = {"config":{"wide_screen_mode":True},"elements":contents, "header":{"template":"red","title":title}}
    return ret

def FormatCardOfNewGaiNian(date,gainian, stocks,titles):
    elements = []
    tag = {"tag":"hr"}
    stockHead = {"tag":"column_set","flex_mode":"none","background_style":"grey","columns":[{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[0],"text_align":"center"}]},{"tag":"column","width":"weighted","weight":1,"vertical_align":"top","elements":[{"tag":"markdown","content":titles[1],"text_align":"center"}]}]}
    elements.append(tag)
    elements.append(stockHead)

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

def _markdownFontColor(data,color = "red"):
    return f'''**<font color='{color}'>{data}</font>**'''

def _markdownFontHighlight(data):
    return f'''**{data}**'''

def _markdown(data):
    tag = {"tag":"markdown","content":data}
    return tag



def FormatCardOfMeiRiFuPan(date,df,redian,redianDfs):
    elements = []
    tag = {"tag":"hr"}
    avgLianBanCount = f'''{df["平均连板数量"][0]:.1f}'''

    line1 = f'''{_markdownFontHighlight("一. 市场总体情况:")}\n**1.** 今成交量:{_markdownFontColor(df["两市量"][0])},增量:{_markdownFontColor(df["增量"][0])}\n**2.** 红盘:{_markdownFontColor(df["红盘"][0])}, 绿盘: {_markdownFontColor(df["绿盘"][0],"green")}\n**3.** 涨停: {_markdownFontColor(df["实际涨停"][0])}, 跌停: {_markdownFontColor(df["跌停"][0])}\n**4.** 炸板: {_markdownFontColor(df["炸板"][0])}，炸板率: {_markdownFontColor(df["炸板率"][0])}\n**5.** 连板个数: {_markdownFontColor(df["连板数量"][0])},近30个交易日最多连板个数: {_markdownFontColor(df["最高连板数量"][0])},最少连板个数: {_markdownFontColor(df["最低连板数量"][0])},平均连板个数: {_markdownFontColor(avgLianBanCount)}'''
    elements.append(_markdown(line1))
    elements.append(tag)

    line2 = f'''{_markdownFontHighlight("二. 市场情绪:")}\n今日势能:{_markdownFontColor(df["势能EX"][0])}, 动能:{_markdownFontColor(df["动能EX"][0])}, 连板股的红盘比:{_markdownFontColor(df["连板股的红盘比"][0])}, 首板红盘比:{_markdownFontColor(df["首板红盘比"][0])}, 备注:{_markdownFontColor(df["备注"][0])}\n\n**情绪判断标准**\n**1. 高潮:** {_markdownFontColor("动能综合值=12 且 势能综合值=10 或者 连板股的红盘比 >=0.78 首板股的红盘比 >=0.78","green")}\n**2. 半高潮:** {_markdownFontColor("只有 连板股的红盘比 >=0.78","green")}\n**3. 冰点期判断 - 强势行情:** {_markdownFontColor("如果动能综合值 =-12 且 势能综合值 <=-2 或者 (动能综合值<=-8 且 势能综合值<=-2) 出现两次","green")}\n**4. 冰点期判断 - 弱势行情:** {_markdownFontColor("如果动能综合值 <=-8 且 势能综合值 =-10 且首板赚钱效应和连板赚钱效应都出现过 <0.4 或者 连续两天动能综合值和势能综合值都<=-6","green")}'''
    elements.append(_markdown(line2))
    elements.append(tag)

    line3 = f'''{_markdownFontHighlight("三. 超短情况:")}\n**1.**10CM首板奖励率:{_markdownFontColor(df["10CM首板奖励率"][0])}%,10CM连板奖励率:{_markdownFontColor(df["10CM连板奖励率"][0])}%\n**2.**20CM首板奖励率:{_markdownFontColor(df["20CM首板奖励率"][0])}%,20CM连板奖励率:{_markdownFontColor(df["20CM连板奖励率"][0])}%\n**3.**2连板个股:{_markdownFontColor(df["2连板个数"][0])}个,近30日最大2连板个股:{_markdownFontColor(df["最大2连板个数"][0])}个\n**4.**3连板个股:{_markdownFontColor(df["3连板个数"][0])}个: {_markdownFontColor(df["3连个股"][0])}\n**5.**4连及以上个股: {_markdownFontColor(df["4连板及以上个数"][0])}个,{_markdownFontColor(df["4连及以上个股"][0])}\n**6.**近30个交易日高度板: {_markdownFontColor(df["最大高度板"][0])} 板'''
    elements.append(_markdown(line3))
    elements.append(tag)

    line4 = f"**四. 今日热点板块:** {_markdownFontColor(redian)}"
    elements.append(_markdown(line4))
    elements.append(tag)

    beizhu = {"elements":[{"content":"风险提示: 本内容仅信息分享,不构成投资建议,若以此作为买卖依据,后果自负。市场有风险,投资需谨慎！","tag":"plain_text"}],"tag":"note"}
    elements.append(beizhu)
    title = f" 每日复盘 - {date}"
    return {"config":{"wide_screen_mode":True},"elements":elements,"header":{"template":"red","title":{"content":title,"tag":"plain_text"}}}
