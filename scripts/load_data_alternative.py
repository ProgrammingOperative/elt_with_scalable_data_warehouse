import psycopg2
import pandas as pd
from sqlalchemy import create_engine

conn = psycopg2.connect(
    database="postgres-db", 
    host="localhost", 
    port="5432", 
    user="postgres", 
    password="docker")

'''
The class cursor allows interaction with the database'''

cur = conn.cursor()

delete_script = "DROP TABLE IF EXISTS traffic;"


create_script = ''' 
    CREATE TABLE IF NOT EXISTS traffic(
        track_id numeric, 
        type varchar (100), 
        traveled_d float,
        avg_speed float, 
        lat float, 
        lon float,
        speed float, 
        lon_acc float, 
        lat_acc float, 
        time float
   );
    '''

cur.execute(delete_script)
cur.execute(create_script)
insert_script = ''' 
    INSERT INTO traffic(
        track_id, 
        type, 
        traveled_d,
        avg_speed, 
        lat, 
        lon,
        speed, 
        lon_acc, 
        lat_acc, 
        time)

    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

'''
Orient = "record" provides a list of dictionaries'''
for record in df.to_dict(orient="records"):
    values = list(record.values())
    # print (values)
    cur.execute(insert_script, values)
    cur.execute("commit")
conn.commit()
conn.close()

engine = create_engine('postgresql://postgres:test1234@localhost/traffic')

df.to_sql("traffic", con=engine, if_exists='replace', index_label='id')
print("<<<<<<<<<<<<<<<<<<<completed>>>>`>>>>>>>>>>>>")