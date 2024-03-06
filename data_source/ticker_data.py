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

# Load ticker symbols from yaml file
with open('finance_portfel/ticker_symbols.yaml', 'r') as file:
    ticker_symbols = yaml.safe_load(file)

tickers = ticker_symbols['ticker_symbols']

def get_ticker_data():
    # Initialize a list to store DataFrames
    dfs_info = []
    dfs_price = []

    # Get the data of the ticker
    for ticker in tickers:
        tickerData = yf.Ticker(ticker)

        # Get the whole information of the stock
        info = tickerData.info

        # Create a DataFrame about the stock ticker information
        df_info = pd.DataFrame({
                'symbol': [info.get('symbol', '')],
                'type': [info.get('quoteType', '')],
                'name': [info.get('longName', '')],
                'description': [info.get('description', '')],
                'market_cap': [info.get('marketCap', '')],
                'currency': [info.get('currency', '')],
                'open': [info.get('open', '')],
        })

        # Convert 'market_cap' and 'open' columns to float
        df_info['market_cap'] = pd.to_numeric(df_info['market_cap'], errors='coerce')
        df_info['open'] = pd.to_numeric(df_info['open'], errors='coerce')

        # Add the DataFrame to the list
        dfs_info.append(df_info)

        # Get the historical prices for this ticker
        df_price = tickerData.history(period='1d', start='2020-01-02', end=current_date)[['Open']]
        df_price['Symbol'] = ticker

        # Set 'Date' as the index and resample the DataFrame to fill in empty dates
        df_price = df_price.groupby('Symbol').apply(lambda x: x.asfreq('D'))

        # Reset the index
        df_price = df_price[['Open']].reset_index()
        df_price['Date'] = df_price['Date'].dt.date

        # Fill in empty 'Open' values with the previous day's data
        df_price['Open'] = df_price.groupby('Symbol')['Open'].fillna(method='ffill')
        
        # Change Date to datetime
        df_price['Date'] = pd.to_datetime(df_price['Date'], format='%Y-%m-%d').dt.strftime('%d/%m/%Y')

        # Create merge_key
        df_price['merge_key'] = df_price['Symbol'].str.split('.').str[0] + '|' + df_price['Date'].astype(str)

        # Add the DataFrame to the list
        dfs_price.append(df_price)

    # Concatenate all the DataFrames in the list
    df_info_all = pd.concat(dfs_info)
    df_price_all = pd.concat(dfs_price)

    # Results
    df = {'ticker_info_data': df_info_all, 'ticker_price_data': df_price_all}

    return df

if __name__ == '__main__':
    get_ticker_data()

    