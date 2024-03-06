'''
Desc: module to get xtb main data from excel extract file stored on local drive
Note: need to change to some sharepoint site
'''
import yaml
import numpy as np
import pandas as pd
import yfinance as yf
import datetime
from openpyxl import load_workbook
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.fundamentaldata import FundamentalData

# Get the current date
current_date = datetime.date.today()

def get_currency_data():
    # Load ticker symbols from yaml file
    with open('finance_portfel/ticker_symbols.yaml', 'r') as file:
        ticker_symbols = yaml.safe_load(file)

    tickers = ticker_symbols['currency_sumbols']

    # Initialize a list to store DataFrames
    dfs = []

    # Get the data of the ticker
    for ticker in tickers:
        tickerData = yf.Ticker(ticker)

        # Get the historical prices for this ticker
        df = tickerData.history(period='1d', start='2020-01-02', end=current_date)
        df = df[['Open']]
        df['Ticker'] = ticker

        # Set 'Date' as the index and resample the DataFrame to fill in empty dates
        df = df.groupby('Ticker').apply(lambda x: x.asfreq('D'))

        # Reset the index
        df = df[['Open']].reset_index()
        df['Date'] = df['Date'].dt.date

        # Fill in empty 'Open' values with the previous day's data
        df['Open'] = df.groupby('Ticker')['Open'].fillna(method='ffill')
        
        # Add the DataFrame to the list
        dfs.append(df)

    # Concatenate all the DataFrames in the list
    df_all = pd.concat(dfs)

    # Results
    df = {'currency_price_data': df_all}

    # Save the DataFrame to an Excel file
    df_all.to_excel(r'finance_portfel\output\ticker_price_data.xlsx')

    return df