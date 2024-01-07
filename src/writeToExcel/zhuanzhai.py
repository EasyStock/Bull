from openpyxl.styles import Font,Border,Side,Alignment,Font,PatternFill
import pandas as pd
from openpyxl.cell.text import InlineFont
from openpyxl.cell.rich_text import TextBlock, CellRichText
from openpyxl.utils import column_index_from_string


# 边框
border = Border(
    left=Side(border_style='thin', color='000000'),
    right=Side(border_style='thin', color='000000'),
    top=Side(border_style='thin', color='000000'),
    bottom=Side(border_style='thin', color='000000'),
)

# 对齐
alignment_center = Alignment(
         horizontal='center',
         vertical='center',
         text_rotation=0,
         indent=0,
         wrapText=True
     )

alignment_left = Alignment(
         horizontal='left',
         vertical='center',
         text_rotation=0,
         indent=0,
         wrapText=True
     )

#文字竖排
alignment_vertical = Alignment(
    horizontal='center', 
    vertical='center', 
    textRotation=255
    )

class CZhuanZaiDetail(object):
    def __init__(self,dbConnection,tradingDays):
        self.dbConnection = dbConnection
        self.tradingDays = tradingDays
        self.rows = 0
        self.index = 1
        self.contextFontSize = 16
        self.sheetName = None
        self.title = None

    def formatColumnsWidth(self,sheet):
        sheet.column_dimensions['A'].width = 20
        sheet.column_dimensions['B'].width = 15
        sheet.column_dimensions['C'].width = 15
        sheet.column_dimensions['D'].width = 15
        sheet.column_dimensions['E'].width = 15
        sheet.column_dimensions['F'].width = 15
        sheet.column_dimensions['G'].width = 15
        sheet.column_dimensions['H'].width = 15
        sheet.column_dimensions['I'].width = 35
        sheet.column_dimensions['J'].width = 15
        sheet.column_dimensions['K'].width = 35
    
    def formatRowHeight(self,sheet,rowIndex,height):
        sheet.row_dimensions[rowIndex].height=height

    def mergeRow(self,sheet,rowIndex,column1,column2,rowHight = 32):
        mergeCell = f'{column1}{rowIndex}:{column2}{rowIndex}'
        sheet.merge_cells(mergeCell)
        if rowHight > 0:
            self.formatRowHeight(sheet,rowIndex,rowHight)
        c1 = column_index_from_string(column1)
        c2 = column_index_from_string(column2)
        for column in range(c1,c2+1):
            cell = sheet.cell(row = rowIndex, column = column)
            cell.border = border

    def mergeColumn(self,sheet,column,start,end):
        mergeCell = f'{column}{start}:{column}{end}'
        sheet.merge_cells(mergeCell)
        c = column_index_from_string(column)
        for index in range(start,end+1):
            cell = sheet.cell(row = index, column = c)
            cell.border = border

    def addTitle(self,sheet):
        self.mergeRow(sheet,1,"A","K",68)
        cell = sheet.cell(1,1)
        cell.alignment = alignment_center
        cell.fill = PatternFill('solid', fgColor="009DDC")
        cell.value =  self.title
        cell.font = Font(name='宋体', size=48, italic=False, color='FFFFFF', bold=True)
        cell.border = border
        self.rows = self.rows + 1

    
    def WriteZhuanZaiInfoToExcel(self,excelWriter,threshold = 2000):
        sql = f'''SELECT `日期`,`转债代码`,`转债名称`,`现价`,`成交额(万元)`,`PB`,`总市值（亿元)`,`溢价率`,`剩余规模`,`行业` FROM stock.kezhuanzhai where `日期` = "{self.tradingDays[-1]}" and `成交额(万元)` > {threshold} and `转债代码` in (SELECT `转债代码` FROM stock.kezhuanzhai where `日期` = "{self.tradingDays[-2]}" and `成交额(万元)` < {threshold})'''
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)
        sheetName = "可转债复盘"
        df.to_excel(excelWriter, sheet_name= sheetName,index=False,startrow=1)
        sheet = excelWriter.sheets[sheetName]
        cell = sheet.cell(1,1)
        cell.alignment = alignment_center
        cell.fill = PatternFill('solid', fgColor="B5C6EA")
        cell.value =  f"{self.tradingDays[-1]} 可转债成交额大于{threshold}"
        cell.font = Font(name='宋体', size=28, italic=False, color='000000', bold=True)
        cell.border = border
        self.mergeRow(sheet,1,"A","J",40)
        startRow = 1
        endRow = startRow + df.shape[0] +1
        for index in range(1,endRow):
            for column in range(1,11):
                cell = sheet.cell(row = startRow + index, column = column)
                cell.border = border
                cell.alignment = alignment_center
                cell.fill = PatternFill('solid', fgColor="000000")
                if index % 2 != 0:
                    cell.fill = PatternFill('solid', fgColor="CCEEFF")
                cell.font = Font(name='宋体', size=12, italic=False, color='000000', bold=True)
                cell.border = border


        self.formatColumnsWidth(sheet)