# Import necessary libraries
import os
import time
import subprocess
import numpy as np
import pandas as pd
from utils import conv_timedelta, conv_type, df_man, df_datetime
import datetime as dt
import glob
import zipfile
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
import requests
import io
pd.set_option('future.no_silent_downcasting', True)


def acquire(date_start, date_end):
    # مسار الحفظ (نفس منطقك القديم)
    my_path = os.path.abspath(os.path.dirname(__file__))
    my_path = os.path.dirname(my_path)
    my_path = os.path.join(my_path, 'data/flight_data')
    if not os.path.exists(my_path): os.makedirs(my_path)

    # تحويل التواريخ لنطاق شهور
    ds_y, ds_m = map(int, date_start.split('-'))
    de_y, de_m = map(int, date_end.split('-'))
    
    base_url = "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"

    print('Downloading starting...')
    curr_y, curr_m = ds_y, ds_m
    
    while (curr_y < de_y) or (curr_y == de_y and curr_m <= de_m):
        url = base_url.format(year=curr_y, month=curr_m)
        r = requests.get(url, stream=True)
        if r.status_code == 200:
            file_name = f"On_Time_Performance_{curr_y}_{curr_m}.zip"
            save_full_path = os.path.join(my_path, file_name)
            with open(save_full_path, 'wb') as f:
                f.write(r.content)
            print(f'Downloaded: {file_name}')
        else:
            print(f'Failed to download month {curr_m} of {curr_y}')
        
        curr_m += 1
        if curr_m > 12:
            curr_m = 1
            curr_y += 1
            
    my_path2 = os.path.abspath(os.path.dirname(__file__))
    return my_path, my_path2


def combine_csv(path1, path2):
    print("بدء فك ضغط الملفات...")
    # 1. فك ضغط جميع ملفات zip في المجلد
    zip_files = glob.glob(os.path.join(path1, "*.zip"))
    for f in zip_files:
        with zipfile.ZipFile(f, 'r') as zip_ref:
            zip_ref.extractall(path1)
        os.remove(f) # حذف الملف المضغوط بعد الفك

    # 2. البحث عن ملفات CSV الناتجة لدمجها
    csv_files = glob.glob(os.path.join(path1, "*.csv"))
    if not csv_files:
        print("خطأ: لم يتم العثور على أي ملفات CSV بعد فك الضغط!")
        return

    # print(f"جاري دمج {len(csv_files)} ملفات...")
    # df_list = [pd.read_csv(f, low_memory=False) for f in csv_files]
    # df_combined = pd.concat(df_list, ignore_index=True)

    # # 3. حفظ الملف النهائي بالاسم الذي يبحث عنه الكود
    # output_path = os.path.join(path1, 'combined_raw_data.csv')
    # df_combined.to_csv(output_path, index=False)
    # print(f"تم إنشاء الملف بنجاح في: {output_path}")

    # # 4. تنظيف المجلد من الملفات الفرعية
    # for f in csv_files:
    #     if 'combined_raw_data.csv' not in f:
    #         os.remove(f)
            





def merge_csv_files(folder_path, output_filename='combined_raw_data.csv'):
    """
    تقوم هذه الدالة بدمج جميع ملفات CSV في مجلد معين وحفظها في ملف واحد
    دون حذف الملفات الأصلية.
    
    Parameters:
    folder_path (str): المسار الذي يحتوي على ملفات CSV
    output_filename (str): اسم الملف النهائي المدمج
    """
    
    # 1. البحث عن جميع ملفات csv في المجلد المذكور
    # نستخدم glob للبحث عن أي ملف ينتهي بـ .csv
    search_pattern = os.path.join(folder_path, "*.csv")
    csv_files = glob.glob(search_pattern)
    
    # التأكد من عدم دمج ملف المخرجات مع الملفات الأصلية إذا كان موجوداً مسبقاً
    csv_files = [f for f in csv_files if output_filename not in f]

    if not csv_files:
        print("لم يتم العثور على ملفات CSV لدمجها.")
        return None

    print(f"جاري دمج {len(csv_files)} ملفات...")

    # 2. قراءة ودمج الملفات
    try:
        # استخدام list comprehension لقراءة الملفات
        df_list = [pd.read_csv(f, low_memory=False) for f in csv_files]
        df_combined = pd.concat(df_list, ignore_index=True)

        # 3. حفظ الملف النهائي
        output_path = os.path.join(folder_path, output_filename)
        df_combined.to_csv(output_path, index=False)
        
        print(f"✅ تم إنشاء الملف بنجاح في: {output_path}")
        print(f"ℹ️ ملاحظة: تم الاحتفاظ بجميع الملفات الأصلية ({len(csv_files)} ملف).")
        
        return df_combined

    except Exception as e:
        print(f"❌ حدث خطأ أثناء الدمج: {e}")
        return None










def import_csv(p_fli, sd, ed):
    file_path = os.path.join(p_fli, 'combined_raw_data.csv')
    
    # قائمة الأعمدة التي نحتاجها بالضبط بالأسماء الأصلية في ملفات 2024
    needed_columns = [
        'DayOfWeek', 'FlightDate', 'Reporting_Airline', 'Tail_Number', 'Flight_Number_Reporting_Airline',
        'OriginAirportID', 'OriginCityMarketID', 'DestAirportID', 'DestCityMarketID',
        'CRSDepTime', 'DepTime', 'DepDelay', 'TaxiOut', 'WheelsOff',
        'WheelsOn', 'TaxiIn', 'CRSArrTime', 'ArrTime', 'ArrDelay',
        'Cancelled', 'CancellationCode', 'Diverted', 'CRSElapsedTime', 'ActualElapsedTime',
        'AirTime', 'Distance', 'CarrierDelay', 'WeatherDelay', 'NASDelay',
        'SecurityDelay', 'LateAircraftDelay'
    ]

    # قراءة الأعمدة المطلوبة فقط لتوفير الذاكرة وتجنب خطأ الـ 110 عمود
    df = pd.read_csv(file_path, usecols=needed_columns, low_memory=False)
    
    # إعادة التسمية للأسماء التي تستخدمها دوال Recover و Integrity في مشروعك
    df.columns = [
        'WeekDay', 'Date', 'IATA', 'TailNum', 'FlightNum',
        'OrgAirID', 'OrgMarID', 'DestAirID', 'DestMarID',
        'ScDepTime', 'DepTime', 'DepDelay', 'TxO', 'WhOff',
        'WhOn', 'TxI', 'ScArrTime', 'ArrTime', 'ArrDelay',
        'Cncl', 'CnclCd', 'Div', 'ScElaTime', 'AcElaTime',
        'AirTime', 'Dist', 'CarrDel', 'WeaDel', 'NASDel',
        'SecDel', 'LatAirDel'
    ]

#      # تقليل حجم الذاكرة (من كودك الذكي)
    df['Cncl'] = df['Cncl'].astype(bool)
    df['Div'] = df['Div'].astype(bool)

    # تحميل بيانات المطارات المساعدة
    p_misc = os.path.join(os.path.dirname(p_fli), 'misc')
    airport = pd.read_csv(os.path.join(p_misc, 'airport.csv'), low_memory=False)
    airport.rename(columns={
    'AIRPORT_ID': 'AirID', 
    'UTC_LOCAL_TIME_VARIATION': 'UTC'}, inplace=True)
    icao = pd.read_csv(os.path.join(p_misc, 'icao.csv'), low_memory=False)
    
    return df, airport, icao, len(df), len(df.columns)


def init_check(df, airport, sd, ed, d):
    """Initially checks all entries in data frames whether
    they are out of bound, null entries, or both, then flags them.

    Returns df : pandas data frame 
                airline on-time performance
            airport : pandas data frame
                airport information
            flag : dictionary
                column flags 1 for null, 2 for out of bound, and 3 for both
            date3 : pandas series
                values of date column under flag 3. Will be removed later on

    Parameters
    ----------
    df : pandas data frame
        Airline on-time performance data
    airport : pandas data frame
        Airport data
    sd : string
        Start Date
    ed : string
        End date
    d : int
        Number of columns in df
    """

    flag = dict(zip(df.columns, np.zeros(d)))

    # List of numerical columns
    num_list = ['WeekDay', 'FlightNum', 'OrgAirID', 'OrgMarID',
                'DestAirID', 'DestMarID', 'ScDepTime', 'DepTime',
                'DepDelay', 'TxO', 'WhOff', 'WhOn', 'TxI', 'ScArrTime',
                'ArrTime', 'ArrDelay', 'Cncl', 'Div', 'ScElaTime',
                'AcElaTime', 'AirTime', 'Dist']

    # Turn off warning, since our goal is to change original data frame
    pd.set_option('mode.chained_assignment', None)
    # Convert all entries to numeric, return NaN if it is not numeric
    for i in num_list:
        df.loc[:, i] = pd.to_numeric(df.loc[:, i], errors='coerce')

    # Weekday has to be in between 1 and 7
    if df.WeekDay.isnull().sum() > 0:
        flag['WeekDay'] = 1
        if df[(df.WeekDay > 7) | (df.WeekDay < 1)].shape[0] > 0:
            flag['WeekDay'] = 3
    elif df[(df.WeekDay > 7) & (df.WeekDay < 1)].shape[0] > 0:
        flag['WeekDay'] = 2

    per = len(pd.period_range(sd, ed, freq='M'))
    ms = pd.date_range(sd, periods=per, freq='MS')[0].strftime('%Y-%m-%d')
    me = pd.date_range(sd, periods=per, freq='M')[-1].strftime('%Y-%m-%d')
    dt_range = list(pd.date_range(ms, me).strftime('%Y-%m-%d').values)
    date3 = []  # Initialize
    # Check incorrect and null date entries
    if df.Date.isnull().sum() > 0:
        flag['Date'] = 1
        if df.Date.isin(dt_range).sum() > 0:
            date3 = df.loc[~df.Date.isin(dt_range), 'Date']
            flag['Date'] = 3
    elif (~df.Date.isin(dt_range)).sum() > 0:
        date3 = df.loc[~df.Date.isin(dt_range), 'Date']
        flag['Date'] = 2

    # There is no way to recover null IATA entries, so it is best to remove them
    if df.IATA.isnull().sum() > 0:
        flag['IATA'] = 1

    # There is no way to recover null TailNum entries, so it is best to remove them
    if df.TailNum.isnull().sum() > 0:
        flag['TailNum'] = 1

    # AirportID varies between 10001 and 16878 (99999 for unknown)
    # CityMarketId varies between 30001 and 36845 (99999 for unknown)

    if df.OrgAirID.isnull().sum() > 0:
        flag['OrgAirID'] = 1
        if ((df.OrgAirID < 10001) | (df.OrgAirID > 16878)).sum() > 0:
            flag['OrgAirID'] = 3
    elif ((df.OrgAirID < 10001) | (df.OrgAirID > 16878)).sum() > 0:
        flag['OrgAirID'] = 2

    if df.DestAirID.isnull().sum() > 0:
        flag['DestAirID'] = 1
        if ((df.DestAirID < 10001) | (df.DestAirID > 16878)).sum() > 0:
            flag['DestAirID'] = 3
    elif ((df.DestAirID < 10001) | (df.DestAirID > 16878)).sum() > 0:
        flag['DestAirID'] = 2

    if df.OrgMarID.isnull().sum() > 0:
        flag['OrgMarID'] = 1
        if ((df.OrgMarID < 30001) | (df.OrgMarID > 36845)).sum() > 0:
            flag['OrgMarID'] = 3
    elif ((df.OrgMarID < 30001) | (df.OrgMarID > 36845)).sum() > 0:
        flag['OrgMarID'] = 2

    if df.DestMarID.isnull().sum() > 0:
        flag['DestMarID'] = 1
        if ((df.DestMarID < 30001) | (df.DestMarID > 36845)).sum() > 0:
            flag['DestMarID'] = 3
    elif ((df.DestMarID < 30001) | (df.DestMarID > 36845)).sum() > 0:
        flag['DestMarID'] = 2

    # Before assigning datetime object to ScDepTime, DepTime, ScArrTime,
    # WhOff, WhOn, and ArrTime, it must be in between 0 and 2400

    if df.ScDepTime.isnull().sum() > 0:
        flag['ScDepTime'] = 1
        if ((df.ScDepTime < 0) | (df.ScDepTime > 2400)).sum() > 0:
            flag['ScDepTime'] = 3
    elif ((df.ScDepTime < 0) | (df.ScDepTime > 2400)).sum() > 0:
        flag['ScDepTime'] = 2

    if df.DepTime.isnull().sum() > 0:
        flag['DepTime'] = 1
        if ((df.DepTime < 0) | (df.DepTime > 2400)).sum() > 0:
            flag['DepTime'] = 3
    elif ((df.DepTime < 0) | (df.DepTime > 2400)).sum() > 0:
        flag['DepTime'] = 2

    if df.WhOff.isnull().sum() > 0:
        flag['WhOff'] = 1
        if ((df.WhOff < 0) | (df.WhOff > 2400)).sum() > 0:
            flag['WhOff'] = 3
    elif ((df.WhOff < 0) | (df.WhOff > 2400)).sum() > 0:
        flag['WhOff'] = 2

    if df.WhOn.isnull().sum() > 0:
        flag['WhOn'] = 1
        if ((df.WhOn < 0) | (df.WhOn > 2400)).sum() > 0:
            flag['WhOn'] = 3
    elif ((df.WhOn < 0) | (df.WhOn > 2400)).sum() > 0:
        flag['WhOn'] = 2

    if df.ScArrTime.isnull().sum() > 0:
        flag['ScArrTime'] = 1
        if ((df.ScArrTime < 0) | (df.ScArrTime > 2400)).sum() > 0:
            flag['ScArrTime'] = 3
    elif ((df.ScArrTime < 0) | (df.ScArrTime > 2400)).sum() > 0:
        flag['ScArrTime'] = 2

    if df.ArrTime.isnull().sum() > 0:
        flag['ArrTime'] = 1
        if ((df.ArrTime < 0) | (df.ArrTime > 2400)).sum() > 0:
            flag['ArrTime'] = 3
    elif ((df.ArrTime < 0) | (df.ArrTime > 2400)).sum() > 0:
        flag['ArrTime'] = 2

    # Cancellation columns has to be 0 or 1
    if df.Cncl.isnull().sum() > 0:
        flag['Cncl'] = 1
        if (~df.Cncl.isin([0, 1])).sum() > 0:
            flag['Cncl'] = 3
    elif (~df.Cncl.isin([0, 1])).sum() > 0:
        flag['Cncl'] = 2

    # Cancellation codes are A, B, C, and D. Assign 0 to null entries
    df['CnclCd'] = df['CnclCd'].replace([np.nan, 'A', 'B', 'C', 'D'], [0, 1, 2, 3, 4])

    if df.CnclCd.isnull().sum() > 0:
        flag['CnclCd'] = 1
        if (~df.CnclCd.isin(range(5))).sum() > 0:
            flag['CnclCd'] = 3
    elif (~df.CnclCd.isin(range(5))).sum() > 0:
        flag['CnclCd'] = 2

    # Div column has to be 0 or 1
    if df.Div.isnull().sum() > 0:
        flag['Div'] = 1
        if (~df.Div.isin([0, 1])).sum() > 0:
            flag['Div'] = 3
    elif (~df.Div.isin([0, 1])).sum() > 0:
        flag['Div'] = 2

    if df.ScElaTime.isnull().sum() > 0:
        flag['ScElaTime'] = 1

    # Fill null rows under all delay columns
    df.iloc[:, 26:] = df.iloc[:, 26:].fillna(0)
    print("Initial check is completed")
    return df, airport, flag, date3


def reval_nan(df):
    """This function checks whether null entries are
    related to cancelled and diverted flights or not.
    If they are related, it leaves them as it is, if
    it is not, it tries to recover them.

    Returns df : pandas data frame 
                Airline on-time performance

    Parameters
    ----------
    df : pandas data frame
        Airline on-time performance data
    """
    # These columns are null when a flight is cancelled
    canc_cols = ['DepTime', 'DepDelay', 'TxO', 'WhOff', 'WhOn',
                 'TxI', 'ArrTime', 'ArrDelay', 'AcElaTime', 'AirTime']
    # Assign nan values to the entries where it is supposed to be
    for col in canc_cols:
        df.loc[(df.Cncl == 1) & (df[col].notna()), canc_cols] = np.nan
    # These columns are null when a flight is cancelled
    div_cols = ['WhOn', 'TxI', 'ArrTime', 'ArrDelay', 'AcElaTime', 'AirTime']
    # Assign nan values to the entries where it is supposed to be
    for col in div_cols:
        df.loc[(df.Div == 1) & df[col].notna(), div_cols] = np.nan

    # A condition for missing data
    def cond(x): return (pd.isna(df[x])) & ((df.Cncl != 1) | (df.Div != 1))
    df.loc[cond('DepTime'), 'DepTime'] = df.loc[cond('DepTime'),
                                                'ScDepTime'] + df.loc[cond('DepTime'), 'DepDelay']
    # df.loc[cond('DepTime'), 'DepTime'] = pd.NaT
    df.loc[cond('DepDelay'), 'DepDelay'] = df.loc[cond('DepDelay'),
                                                  'ScDepTime'] - df.loc[cond('DepDelay'), 'DepTime']
    # df.loc[cond('DepDelay'), 'DepDelay'] = pd.NaT
    df.loc[cond('TxO'), 'TxO'] = df.loc[cond('TxO'), 'WhOff'] - \
        df.loc[cond('TxO'), 'DepTime']
    # df.loc[cond('TxO'), 'TxO'] = pd.NaT
    df.loc[cond('WhOff'), 'WhOff'] = df.loc[cond('WhOff'),
                                            'DepTime'] + df.loc[cond('WhOff'), 'TxO']
    # df.loc[cond('WhOff'), 'WhOff'] = pd.NaT
    df.loc[cond('WhOn'), 'WhOn'] = df.loc[cond('WhOn'),
                                          'ArrTime'] - df.loc[cond('WhOn'), 'TxI']
    # df.loc[cond('WhOn'), 'WhOn'] = pd.NaT
    df.loc[cond('TxI'), 'TxI'] = df.loc[cond('TxI'),
                                        'ArrTime'] - df.loc[cond('TxI'), 'WhOn']
    # df.loc[cond('TxI'), 'TxI'] = pd.NaT
    df.loc[cond('ArrTime'), 'ArrTime'] = df.loc[cond('ArrTime'),
                                                'ScArrTime'] + df.loc[cond('ArrTime'), 'ArrDelay']
    # df.loc[cond('ArrTime'), 'ArrTime'] = pd.NaT
    df.loc[cond('ArrDelay'), 'ArrDelay'] = df.loc[cond('ArrDelay'),
                                                  'ArrTime'] - df.loc[cond('ArrDelay'), 'ScArrTime']
    # df.loc[cond('ArrDelay'), 'ArrDelay'] = pd.NaT
    df.DepTime = df.ScDepTime + df.DepDelay
    df.WhOff = df.DepTime + df.TxO
    mask = df.ScDepTime > df.ScArrTime
    df.loc[mask, 'ScArrTime'] = df.loc[mask, 'ScDepTime'] + df.loc[mask, 'ScElaTime'] + df.loc[mask, 'TimeZoneDiff']
    df.ArrTime = df.ScArrTime + df.ArrDelay
    df.WhOn = df.ArrTime - df.TxI

    return df


def recover(df, airport, flag, date3, n, d):
    """Removes and/or imputes data if specific conditions are met.


    Returns df : pandas data frame 
                Airline on-time performance

    Parameters
    ----------
    df : pandas data frame 
        Airline on-time performance
    airport : pandas data frame
        Airport information
    flag : dictionary
        Column flags 1 for null, 2 for out of bound, and 3 for both
    date3 : pandas series
        Values of date column under flag 3. Will be removed later on
    n : int
        Number of rows in df
    d : int
        Number of columns in df
    """
    st = time.time()
    print("Recovering data has started", end="\r")
    # Recovering WeekDay using Date column only for null entries
    if flag['WeekDay'] == 1:
        cond = df.WeekDay.isnull()
        t1 = df.loc[cond, 'Date']
        if t1.isnull().sum() > 0:
            # Return nan if NaT
            df.loc[cond, 'WeekDay'] = t1.dt.dayofweek + 1  # Zero indexed
            df = df.loc[df.WeekDay.notna(), :]

    # Recovering WeekDay using Date column for null and out of range entries
    elif flag['WeekDay'] == 3:
        cond = (df.WeekDay.isnull()) | (df.WeekDay < 1) | (df.WeekDay > 7)
        t1 = df.loc[cond, 'Date']
        if t1.isnull().sum() >= 0:
            # Return nan if NaT
            df.loc[cond, 'WeekDay'] = t1.dt.dayofweek + 1
            # Check there is still out of range entries
            cond = (df.WeekDay > 7) | (df.WeekDay < 1)
            # out of range + null entries
            df = df.loc[(df.WeekDay.notna()) | (df.WeekDay.loc[~cond]), :]

    # Recovering WeekDay using Date column only for out of range entries
    elif flag['WeekDay'] == 2:
        cond = (df.WeekDay < 1) | (df.WeekDay > 7)
        t1 = df.loc[cond, 'Date']
        if t1.isnull().sum() >= 0:
            # Return nan if NaT
            df.loc[cond, 'WeekDay'] = t1.dt.dayofweek + 1
            # Check there is still out of range entries
            cond = (df.WeekDay > 7) | (df.WeekDay < 1)
            # out of range + null entries
            df = df.loc[(df.WeekDay.notna()) | (df.WeekDay.loc[~cond]), :]

    # Removing null or out of range Date, IATA, TailNum, OrgAirID, 
    # DestAirID entries because not way to recover it
    if flag['Date']:
        df = df.loc[df['Date'].notna(), :]
        df = df.loc[~df['Date'].isin(date3), :]

    if flag['IATA']:
        df = df.loc[df.IATA.notna(), :]

    if flag['TailNum']:
        df = df.loc[df.TailNum.notna(), :]

    if flag['OrgAirID']:
        df = df.loc[df.OrgAirID.notna(), :]
        cond = (df.OrgAirID <= 10001) | (df.OrgAirID >= 16878)
        df = df.loc[~cond, :]

    if flag['DestAirID']:
        df = df.loc[df.DestAirID.notna(), :]
        cond = (df.DestAirID <= 10001) | (df.DestAirID >= 16878)
        df = df.loc[~cond, :]

    # Recovering City Market ID's using supplementary data frame, airport
    if flag['OrgMarID']:
        cond = (df.OrgMarID <= 30001) | (
            df.OrgMarID >= 36845) | (df.OrgMarID.isnull())
        ntemp = cond.sum()
        array = df.loc[cond, 'OrgAirID'].drop_duplicates().values
        names = airport[airport.AirID.isin(array)][['AirID', 'CityMarketID']]
        for idx, val in names.iterrows():
            # np.where is much faster!
            df.OrgMarID = np.where(
                cond | (df.OrgAirID == val[0]), val[1], df.OrgMarID.values)
        # Recheck condition (In case both OrgAirID and OrgMarID is not available)
        cond = (df.OrgMarID <= 30001) | (
            df.OrgMarID >= 36845) | (df.OrgMarID.isnull())
        df = df.loc[~cond, :]

    if flag['DestMarID']:
        cond = (df.DestMarID <= 30001) | (
            df.DestMarID >= 36845) | (df.DestMarID.isnull())
        ntemp = cond.sum()
        array = df.loc[cond, 'DestAirID'].drop_duplicates().values
        names = airport[airport.AirID.isin(array)][['AirID', 'CityMarketID']]
        for idx, val in names.iterrows():
            # np.where is much faster!
            df.DestMarID = np.where(
                cond | (df.DestAirID == val[0]), val[1], df.DestMarID.values)
        # Recheck condition (In case both DestAirID and DestMarID are not available)
        cond = (df.DestMarID <= 30001) | (
            df.DestMarID >= 36845) | (df.DestMarID.isnull())
        df = df.loc[~cond, :]

    if flag['Div']:
        cond = (df.Div.isnull()) | (df.Div > 1) | (df.Div < 0)
        cols = ['ArrTime', 'ArrDelay', 'AcElaTime', 'AirTime', 'TxI', 'WhOn']
        var = df.loc[cond, :][cols]
        if (var.isnull().sum().isin([6]).sum()) == (df.loc[cond, :].shape[0]):
            df.loc[cond, 'Div'] = 1
        else:
            df = df.loc[~cond, :]
        df.loc[df.Div == 1, cols] = np.nan

    df.loc[df.Div == 1, 'CnclCd'] = 0

    if flag['Cncl']:
        cond = (df.Cncl.isnull()) | (df.Cncl > 1) | (df.Cncl < 0)
        cols = ['DepTime', 'DepDelay', 'TxO', 'WhOff', 'ArrTime',
                'ArrDelay', 'AcElaTime', 'AirTime', 'TxI', 'WhOn']
        var = df.loc[cond, :][cols]
        if (var.isnull().sum().isin([10]).sum()) == (df.loc[cond, :].shape[0]):
            df.loc[cond, 'Cncl'] = 1
        else:
            df = df.loc[~cond, :]

    cols = ['DepTime', 'DepDelay', 'TxO', 'WhOff', 'ArrTime',
            'ArrDelay', 'AcElaTime', 'AirTime', 'TxI', 'WhOn']
    df.loc[df.Cncl == 1, cols] = np.nan

    # There is no way to guess cancellation code
    if flag['CnclCd']:
        cond = (df.CnclCd.notna()) | (df.CnclCd.isin(range(5)))
        df = df.loc[cond, :]
    # There is no way to guess Scheduled Departure Time
    if flag['ScDepTime']:
        cond = (df.ScDepTime.isnull()) | (
            df.ScDepTime < 0) | (df.ScDepTime > 2400)
        df = df.loc[~cond, :]
    # There is no way to guess Scheduled Arrival Time
    if flag['ScArrTime']:
        cond = (df.ScArrTime.isnull()) | (
            df.ScArrTime < 0) | (df.ScArrTime > 2400)
        df = df.loc[~cond, :]
    # There is no way to guess Scheduled Elapsed Time
    if flag['ScElaTime']:
        cond = df.ScElaTime.isnull()
        df = df.loc[~cond, :]

    # Let's shrink size of the dataframe!
    # بدلاً من 'int8' استخدم 'Int8' وبدلاً من 'int16' استخدم 'Int16'
    types = {0: 'float32', 1: object, 2: 'category', 3: 'category', 4: 'float32', 5: 'float32',
         6: 'float32', 7: 'float32', 8: 'float32', 19: 'bool', 20: 'category', 21: 'bool',
         25: 'float32', 26: 'float32', 27: 'float32', 28: 'float32', 29: 'float32', 30: 'float32'}
    
    for key, val in types.items():
        conv_type(df, key, val)

    # التأكد من أسماء الأعمدة الصحيحة لملف 2024 قبل البدء
    if 'AIRPORT_ID' in airport.columns:
        airport.rename(columns={'AIRPORT_ID': 'AirID', 'UTC_LOCAL_TIME_VARIATION': 'UTC'}, inplace=True)

    # تعريف أنواع البيانات لجدول المطارات
    # تم استخدام الأسماء بدلاً من الأرقام لضمان الدقة حتى لو تغير ترتيب الأعمدة
    types_air = {
        'AirID': 'int32', 
        'UTC': 'float32'
    }
    
    # تنظيف وتحويل الأعمدة الرقمية (Latitude, Longitude, UTC)
    # ملاحظة: تأكد من مراجعة أسماء أعمدة الإحداثيات في ملفك (مثلاً 'LATITUDE') إذا أردت تضمينها
    cols_to_fix = ['UTC']
    
    for col in cols_to_fix:
        if col in airport.columns:
            airport[col] = pd.to_numeric(airport[col], errors='coerce')
    
    # تنفيذ تحويل الأنواع للجزء المعرف في القاموس
    for col_name, col_type in types_air.items():
        if col_name in airport.columns:
            airport[col_name] = airport[col_name].astype(col_type)
    cols = ['ScDepTime', 'ScArrTime', 'DepTime', 'ArrTime', 'WhOff', 'WhOn']
    for name in cols:
        df[name] = df_man(df[name])

    df['Date'] = df.Date.str.replace('-', '')

    for col_name in cols:
        df[col_name] = df.Date + ' ' + df[col_name]

    cols = ['DepDelay', 'TxO', 'TxI', 'ArrDelay',
        'ScElaTime', 'AcElaTime', 'AirTime']
    for col in cols:
        conv_timedelta(df, col, 'min')

    # No need to have Date column
    df = df.drop(columns=['Date'])
    # Temp assignment for re-evaluation of nan entries
    # Actual arrival time might be the day after
    cols = ['ScDepTime', 'ScArrTime', 'DepTime', 'ArrTime', 'WhOff', 'WhOn']
    for col_name in cols:
        df[col_name] = df_datetime(df[col_name])

    # حذف الرحلات التي لا تحتوي على رقم رحلة (حل مشكلة FlightNum null)
    df = df.dropna(subset=['FlightNum'])

    df = df.reset_index(drop=True)

    utc_org = pd.merge(df, airport[['AirID', 'UTC']], left_on='OrgAirID',
                       right_on='AirID', how='left')['UTC']
    utc_dest = pd.merge(df, airport[['AirID', 'UTC']], left_on='DestAirID',
                        right_on='AirID', how='left')['UTC']
    diff = utc_dest - utc_org
    df['TimeZoneDiff'] = pd.to_timedelta((diff / 100) * 60, unit='m')

    # ---------------------------------------------------------
    # جديد: الخطوة الثانية - إعادة حساب الفوارق الزمنية لضمان تطابق Integrity Check
    # ---------------------------------------------------------
    # حساب الوقت المخطط له بالدقائق (ScElaTime)
    # الصيغة: (الوصول - الإقلاع) محولاً لدقائق - فرق التوقيت بالدقائق
    df['ScElaTime'] = (df['ScArrTime'] - df['ScDepTime']).dt.total_seconds() / 60.0 - (diff / 100) * 60.0
    df['ScElaTime'] = pd.to_timedelta(df['ScElaTime'], unit='m')

    # حساب الوقت الفعلي ووقت الطيران للرحلات التي لم تُلغَ ولم تُحول
    mask = (df['Cncl'] == 0) & (df['Div'] == 0)
    
    # AcElaTime = (الوصول الفعلي - الإقلاع الفعلي) - فرق التوقيت
    df.loc[mask, 'AcElaTime'] = (df.loc[mask, 'ArrTime'] - df.loc[mask, 'DepTime']).dt.total_seconds() / 60.0 - df.loc[mask, 'TimeZoneDiff'].dt.total_seconds() / 60.0
    df.loc[mask, 'AcElaTime'] = pd.to_timedelta(df.loc[mask, 'AcElaTime'], unit='m')

    # AirTime = (وقت ملامسة الأرض - وقت الإقلاع من المدرج)
    df.loc[mask, 'AirTime'] = (df.loc[mask, 'WhOn'] - df.loc[mask, 'WhOff']).dt.total_seconds() / 60.0
    df.loc[mask, 'AirTime'] = pd.to_timedelta(df.loc[mask, 'AirTime'], unit='m')

    # ---------------------------------------------------------
    # جديد: الخطوة الثالثة - حذف الرحلات التي يسبق فيها الإقلاع الوصول المجدول
    # ---------------------------------------------------------
    df = df.loc[df['ScArrTime'] > df['ScDepTime']]


    # Re-evaluate nans
    df = reval_nan(df)
    df = df.reset_index(drop=True)
    et = time.time()
    print("Recovering data has completed in {:.2f} seconds".format(et-st))
    return df.sort_values('ScDepTime')
