from ast import Load
from sqlalchemy import create_engine

import extract_data

from extract_data import ExtractCSV

import pymysql

import pandas as pd

class LoadToDB:
    def __init__(self) -> None:
        self.data = ExtractCSV.load_and_restructure()

    def load_to_db(self):
        
        sqlEngine = create_engine('postgresql+psycopg2://wacira:testing321@localhost:5433/testdb', pool_recycle=3600)

        dbConnection = sqlEngine.connect()

        tableName = "traffic_table"

    

        try:

            frame = self.data.to_sql(tableName, dbConnection, if_exists='replace');

        except ValueError as vx:

            print(vx)

        except Exception as ex:   

            print(ex)

        else:

            print("Table %s created successfully."%tableName);   

        finally:

            dbConnection.close()




