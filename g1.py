import pandas as pd
import numpy as np
from datetime import datetime
import os

__plapid = {
    "台北101購物中心" : '1d56e443604354ed396ea153f8b55846',
    }

def add_plapid(k:str, v:str):
    """
    k: key\n
    v: value\n
    return plapid dic
    """
    __plapid[k] = v
    return __plapid

def __fplis(directory):
    fp = [[np.nan for x in range(0)] for y in range(0)]
    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        if os.path.isfile(f):
            if("detailed" in filename):
                fp.append([f, filename])
    return fp

def __fpplis(directory):
    fpp = [[np.nan for x in range(0)] for y in range(0)]
    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        if os.path.isfile(f):
            if("places" in filename):
                fpp.append([f, filename])
    return fpp

def __findd(fil, ca = "@", fal = np.nan):
    lin = fil['link'][0].split('/')
    for i in lin:
        if (ca in i):
            return i
    return fal

def laln(fil : str) -> str:
    """
    fil: file path to review location csv file\n
    return @latitude,longitude,17z\n
    return @0,0,17z if false
    """
    r = __findd(fil, '@', '@0,0,17z')
    if (r=='@0,0,17z'):
        t = ((__findd(fil, 'data').split(':')[1]).split('?')[0]).split('!')
        for i in t:
            if(i[:2] == '3d'):
                r = f'@{i.replace('3d','')},'
            if(i[:2] == '4d'):
                r += f'{i.replace('4d','')},17z'
                break
    return r

def __pidd(fil):
    p = __findd(fil, 'data').split(':')
    q = p[1].split('!')
    return q[0]

def __topid(pla, flp):
    try:
        return __plapid[pla]
    except:
        pidl = pd.read_csv(flp)
        for i in range(len(pidl)):
            if(pla == pidl['name'][i]):
                __plapid[pla] = pidl['attraction_id'][i]
                return __plapid[pla]

def main(fpd:str, fppd:str, flp:str) -> pd.DataFrame:
    """
    fpd: folder path to review detail csv files\n
    fppd: folder path to review location csv files\n
    flp: file path to location-ID csv file\n
    return dataframe
    """
    col = ['rid', 'pid', 'name', 'rating', 'text', 'source', 'date', 'url']
    fp = __fplis(fr"{fpd}")
    fpp = __fpplis(fr"{fppd}")
    fl1 = [[np.nan for x in range(0)] for y in range(0)]
    for j in range(len(fp)):
        fl = pd.read_csv(fp[j][0])
        p = fp[j][1].replace('detailed-reviews-of-','').replace('.csv','')
        for k in range(len(fpp)):
            l = fpp[k][1].replace('places-of-','').replace('.csv','')
            if(p==l):
                p = fpp[k][0]
                break
        fll = pd.read_csv(p)
        lo = laln(fll)
        gpid = __pidd(fll)
        for i in range(len(fl['review_id'])):
            pid = __topid(fl['place_name'][i], fr"{flp}")
            ti = fl['published_at'][i]
            if(ti == ti):
                ti = datetime.strptime(fl['published_at'][i], '%Y-%m-%d %H:%M:%S').date()
            else:
                ti = np.nan
            rid = fl['review_id'][i]
            url = f'https://www.google.com/maps/reviews/{lo}/data=!3m1!4b1!4m6!14m5!1m4!2m3!1s{rid}!2m1!1s0x0:{gpid}?hl=zh-TW&entry=ttu'
            fl1.append([fl['review_id'][i], pid, fl['user_name'][i], float(fl['rating'][i]), fl['review_text'][i], 'google-maps', ti, url])
    return pd.DataFrame(fl1, columns = col)