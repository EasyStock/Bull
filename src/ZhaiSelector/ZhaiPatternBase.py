
class CZhaiPatternBase(object):
    def __init__(self,dbConnection):
        self.dbConnection = dbConnection

    def PatternName(self) -> str:
        return ""
    

    def getData(self):
        pass

    def SelectLast(self,params:dict):
        pass


    def SelectAll(self,params:dict):
        pass
    

    def Descriptions(self,params:dict):
        pass