from MA.MA1 import EventLast_MA1,Predict_MA1
from MA.MA2 import EventLast_MA2,Predict_MA2
import pandas as pd


class CIndexMAMgr(object):
    def __init__(self,dbConnection):
        self.dbConnection = dbConnection

    
    def _getIndexInfo(self):
        sql = f'''SELECT * FROM stock.kaipanla_index;'''
        results, columns = self.dbConnection.Query(sql)
        df = pd.DataFrame(results,columns=columns)
        return df

    def IndexInfo(self):
        pass