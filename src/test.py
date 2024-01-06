from mysql.connect2DB import ConnectToDB
import pandas as pd
from thsData.fetchZhangTingFromTHS import CFetchZhangTingDataFromTHS


def Test1_BuyTogether(dbConnection,operatorID1, operatorID2):
    sql = f'''select * from `stock`.`dragon` where (operator_ID = {operatorID1} or operator_ID = {operatorID2})  and `flag` = "B" and (date,stockID) in (select date,stockID from `stock`.`dragon` where operator_ID = {operatorID2} and `flag` = "B" and (sell = "nan" or sell = 0) and (date,stockID) in (select date,stockID from `stock`.`dragon` where operator_ID = {operatorID1} and `flag` = "B" and (sell = "nan" or sell = 0)))
'''
    print(sql)
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    print(df)

def Test1_SellTogether(dbConnection,operatorID1, operatorID2):
    sql = f'''select * from `stock`.`dragon` where (operator_ID = {operatorID1} or operator_ID = {operatorID2})  and `flag` = "S" and (date,stockID) in (select date,stockID from `stock`.`dragon` where operator_ID = {operatorID2} and `flag` = "S" and (buy = "nan" or buy = 0) and (date,stockID) in (select date,stockID from `stock`.`dragon` where operator_ID = {operatorID1} and `flag` = "S" and (buy = "nan" or buy = 0)))
'''
    print(sql)
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    print(df)



def format2To3(row):
    ban3 = row['3连板个数']
    zuoRi2Ban = row['昨日2板数']
    ratio = 0.0
    if zuoRi2Ban == 0:
        ratio = 0.0
    else:
        ratio = float(f'''{ban3/zuoRi2Ban:.2f}''')
    
    return ratio

def format3ToHigher(row):
    ban4 = row['4连板及以上个数']
    ban3_zuori = row['昨日3板数']
    ban4_zuori = row['昨日4板及以上个数']
    ratio = 0.0
    if (ban3_zuori + ban4_zuori) == 0:
        ratio = 0.0
    else:
        ratio = float(f'''{ban4/(ban3_zuori + ban4_zuori):.2f}''')
    return ratio


def DataFrameToSqls_UPDATE(datas,tableName,index_str):
    sqls = []
    for index, row in datas.iterrows():
        sql = '''UPDATE %s SET ''' %(tableName)

        for rowIndex, value in row.items():
            sql = sql + '''`%s` = '%s',''' %(rowIndex,value)
        sql = sql[:-1]
        sql = sql + ''' WHERE `%s` = '%s'; '''%(index_str,index)
        sqls.append(sql)
    return sqls

def UpdateData(dbConnection):
    sql =f'''SELECT * FROM stock.fupan;'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)


    df["昨日2板数"] = df["2连板个数"].shift()
    df["昨日3板数"] = df["3连板个数"].shift()
    df["昨日4板及以上个数"] = df["4连板及以上个数"].shift()

    df['2进3成功率'] = df.apply(lambda row: format2To3(row), axis=1)
    df['3进高成功率'] = df.apply(lambda row: format3ToHigher(row), axis=1)
    newDf = pd.DataFrame(df,columns=['日期','2进3成功率','3进高成功率'])
    newDf.set_index(["日期",],drop=True,inplace=True)
    sqls = DataFrameToSqls_UPDATE(newDf,tableName='fupan',index_str='日期')
    #newDf.to_excel('/tmp/222.xlsx')
    for sql in sqls:
        dbConnection.Execute(sql)
        
    print(sqls[:5])


def ReadXLS():
    import openpyxl
    wb = openpyxl.load_workbook('/Volumes/Data/111.xlsx')
    print(wb.sheetnames, '\n')
    wb.add_format({'align': 'text_wrap'})
    active_sheet = wb.active
    cell = active_sheet['A1']
    #print(cell,cell.fill.bgcolor)
    print(cell,cell.value)
    print(cell,cell.font.color)

def WriteXLS():
    # import openpyxl
    # from openpyxl.cell.rich_text import TextBlock, CellRichText
    # from openpyxl.cell.text import InlineFont

    # wb = openpyxl.Workbook()
    # ws = wb.active

    # size_10 = InlineFont(sz=10,color='FF0000')
    # size_20 = InlineFont(sz=20,color='FFFF00')

    # ws["A1"] = CellRichText([TextBlock(font=size_10, text="Apple"), TextBlock(font=size_20, text="Banana")])

    # wb.save(r"/tmp/333.xlsx")

    dbConnection = ConnectToDB()
    import numpy as np
    from openpyxl.utils import get_column_letter
    from openpyxl.styles import Font  # 导入字体模块
    from openpyxl.styles import PatternFill  # 导入填充模块
    from openpyxl.styles import Border,Side,Alignment,Font
    from openpyxl.cell.text import InlineFont
    from openpyxl.cell.rich_text import TextBlock, CellRichText

    sql = f'''SELECT * FROM stock.stockzhangting where `日期` = "2024-01-04" order by `连续涨停天数` DESC;'''
    results, columns = dbConnection.Query(sql)
    df = pd.DataFrame(results,columns=columns)
    fullPath = "/tmp/222.xlsx"
    align = Alignment(horizontal='center',vertical='center',wrap_text=True)
    fille = PatternFill('solid', fgColor= 'ffff00')  # 设置填充颜色为 橙色
    font1 = Font(name='宋体', size=28, italic=False, color="ff0000", bold=True)
    font2 = Font(name='宋体', size=28, italic=False, color="ffFFFF", bold=True)
    with pd.ExcelWriter(fullPath,engine='openpyxl',mode='w+') as excelWriter:
        pd.DataFrame().to_excel(excelWriter, sheet_name="涨停梯队", index=False,startrow=1,header=True)
        #df.to_excel(excelWriter, sheet_name="涨停梯队", index=False,startrow=1,header=True)
        ws = excelWriter.sheets['涨停梯队']
        red = InlineFont(color='FF0000')
        rich_string1 = CellRichText(['When the color ', TextBlock(red, 'red'), ' is used, you can expect ', TextBlock(red, 'danger')])

        ws.cell(1,1).value = rich_string1

    #     mergeCell = f'A{41}:H{41}'
    #     ws.merge_cells(mergeCell)
    #     ws.cell(41,1).value = rich_string1
        
        # ws.cell(1,1).alignment = align
        # ws.cell(1,1).fill = fille
        # #ws.cell(1,1).font = font1
        # ws.row_dimensions[1].height=40
        # ws.column_dimensions['A'].width = 15
        # ws.column_dimensions['B'].width = 15
        # ws.column_dimensions['C'].width = 15
        # ws.column_dimensions['D'].width = 15
        # ws.column_dimensions['E'].width = 85
        # ws.column_dimensions['F'].width = 85
        # ws.column_dimensions['G'].width = 15
        # ws.column_dimensions['H'].width = 15


if __name__ == "__main__":
    #dbConnection = ConnectToDB()
    # Test1_BuyTogether(dbConnection,10028451,10656871)
    # Test1_SellTogether(dbConnection,10028451,10656871)
    #UpdateData(dbConnection)
    #WriteXLS()
    WriteXLS()