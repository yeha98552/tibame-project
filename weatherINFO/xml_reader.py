import pandas as pd
import numpy as np
import xmltodict
mon = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
# data_INFOs = []
def dataread(year,month):
    with open(fr"C:\Users\T14 Gen 3\Documents\tibame-project\weatherData\mn_Report_{year}{month}.xml",encoding="utf-8") as xml_file:
        data_dict = xmltodict.parse(xml_file.read())
        data_INFO = pd.json_normalize(data_dict['cwaopendata']['resources']['resource']['data']['surfaceObs']['location'])
    return data_INFO

def dataget():
  result = pd.DataFrame()
  for year in range(2015,2024):
    for month in mon:
      data = dataread(year,month)
      
      result = pd.concat([result, data], axis=0)
  pd.DataFrame(result)
  result.to_csv(r"C:\Users\T14 Gen 3\Documents\tibame-project\weatherINFO\weatherInfoDWresult.csv", index = False)
  
dataget()