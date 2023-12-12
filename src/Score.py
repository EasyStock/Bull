from score.score import scoreV
from mysql.connect2DB import ConnectToDB




if __name__ == "__main__":
    dbconnection = ConnectToDB()
    scoreV(dbconnection)