import pandas as pd
import numpy as np
import requests

import csv

import time
import datetime

import os.path
import logging



logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s:%(filename)s:%(funcName)s:%(levelno)s:%(message)s')

file_handler = logging.FileHandler('download_data_frame.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)



def download_data_from_renewable(year):

    url = f'https://www.renewable-ei.org/en/statistics/electricity/data/{year}/power-data.json'

    response = requests.get(url)

    if response.url == url:
        data_time = response.json()['epochs']
        converte_date = []
        for i in data_time:
            date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(i))
            converte_date.append(date)
        
        data = response.json()['japan']
        data['Date'] = converte_date
        fieldsnames = [i for i in data.keys()]

        with open(f'files/power_data_{year}.csv', 'w') as file:
            writer = csv.writer(file)
            writer.writerow(fieldsnames)
            for values in zip(*data.values()):
                writer.writerow(values)
            


def download_data_from_jepx(year):

    url = f'http://www.jepx.org/market/excel/index_{year}.csv'
    response = requests.get(url)

    with open(f'files/trading_data_{year}.csv', 'wb') as f:
        f.write(response.content)


def clean_data_renewable(year):
    df = pd.read_csv(f'files/power_data_2015_{year}.csv')
    df = df.astype({'Date': 'datetime64[ns]'})
    df.index = df.Date
    df = df.drop('Date', axis=1)
    df.to_csv(f'files/power_data_2015_{year}.csv')
    return df


def clean_data_jpex(year):
    df = pd.read_csv(f'files/trading_data_2015_{year}.csv')
    df = df.rename(columns={'\x94N\x8c\x8e\x93ú': 'Day', 'DA-24(\/kWh)': 'DA_24'})
    df = df.astype({'Day': 'datetime64[ns]', 'DA_24': 'float'})
    df.index = df.Day
    df = df.drop('Day', axis=1)
    df = df.drop('Unnamed: 0', axis=1)
    return df


def main(year):
    for i in range(2015, year+1):
        download_data_from_renewable(i)
        download_data_from_jepx(i)
    

    concat_renewable = pd.concat([pd.read_csv(f'files/power_data_{years}.csv') for years in range(2015, year+1) if os.path.exists(f'files/power_data_{years}.csv')])
    concat_renewable.to_csv(f'files/power_data_2015_{year}.csv')

    concat_jpex = pd.concat([pd.read_csv(f'files/trading_data_{years}.csv', encoding='ISO-8859-1') for years in range(2015, year+1)])
    concat_jpex.to_csv(f'files/trading_data_2015_{year}.csv')


    data_renewable = clean_data_renewable(year)
    data_jpex = clean_data_jpex(year)

    data_ren_resample = data_renewable.resample('D').last()

    for index in data_jpex.index:
        if not index in data_ren_resample.index:
            data_jpex = data_jpex.drop(index, axis=0)
    
    data_ren_resample['match'] = np.where(data_jpex['DA_24'] == data_ren_resample.spot_price, data_jpex['DA_24'], data_jpex['DA_24'])

    data_ren_resample.to_csv(f'DataFrame_{year}.csv')
    logger.info('Files downloaded')
        

if __name__ == '__main__':
    
    try:
        main(int(datetime.date.today().year))
    except Exception as e:
        logger.error(e)



