import pymysql
from pymysql.err import IntegrityError, InternalError
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

def info():
       df = pd.read_csv(r"C:\Users\T14 Gen 3\Documents\tibame-project\WeatherStanp\weatherStandDW.csv")

       # 設定資料庫連線資訊
       db_data = 'mysql+pymysql://' + 'root' + ':' + 'pasword' + '@' + 'localhost' + ':3306/' \
              + 'TIRDATA' + '?charset=utf8mb4'
       engine = create_engine(db_data)
       df = df.rename(
       {'站號':'station_orig_id',
       '站名':'name',
       '經度':'lat',
       '緯度':'lon',
       '城市':'city',
       '地址':'address'},axis=1
       )

       df.to_sql('weather_station',engine,if_exists='append',index=False)

       # 建立連線
       # conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='pasword', db='TIRDATA', charset='utf8mb4')
       # cursor = conn.cursor()


def station():
       df1 = pd.read_csv(r"C:\Users\T14 Gen 3\Documents\tibame-project\weatherINFO\weatherInfoDW.csv")
       for col in  ['Minimum AirTemperature (˚C)','Maximum AirTemperature (˚C)', 'Mean AirTemperature (˚C)',
                    'Accumulation Precipitation (mm)', 'Minimum RelativeHumidity (%)',
                    'Total SunshineDuration (hr)', 'year', 'month']:
                     df1[col] = pd.to_numeric(df1[col], errors='coerce').replace([np.nan],[None])
       df1.rename(
       {'ID':'city_id'               
       
       },axis=1)
       print(df1)

       # 建立連線
       with pymysql.connect(host='localhost', 
                            port=3306, 
                            user='root', 
                            passwd='pasword', 
                            db='TIRDATA', 
                            charset='utf8mb4') as conn:
              with conn.cursor() as cursor:

                     sql_insert="""
                     INSERT INTO TIRDATA.weather_history (station_id, `year_month`, temperature_low, temperature_high, temperature_avg, rainfall, humidity_avg, total_sunshine_hrs, `year`, `month`)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                     """

                     for idx, row in df1.iterrows():
                            cursor.execute(sql_insert,tuple(row.values))
                     conn.commit()      

info()
station()