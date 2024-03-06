import sys
import pandas as pd
sys.path.append('finance_portfel/')
from data_source.ticker_data import get_ticker_data
from data_source.xtb import get_xtb_main_data


'''
- need to align to one currency yahoo and other sources (probably seperate script)
- need to have seperate script to change date to one format
- need to have seperate script to split symbol
- split closed with open and have few column Open Closed Depost Dividend
- clean up columns so I will not have duplicates w _x 
'''

def stock_etl():
    """
    The main function of the application.
    """
    # Get ticker data
    df_ticker_data = get_ticker_data()
    df_ticker_info = df_ticker_data['ticker_info_data']
    df_ticker_info.columns = map(str.lower, df_ticker_info.columns)

    df_ticker_price = df_ticker_data['ticker_price_data']
    df_ticker_price.columns = map(str.lower, df_ticker_price.columns)

    # Get xtb data
    df_xtb_data = get_xtb_main_data()
    df_xtb_open = df_xtb_data['open']
    df_xtb_closed = df_xtb_data['closed']
    df_xtb_deposit = df_xtb_data['deposit']
    df_xtb_dividend = df_xtb_data['dividend']

    df = df_ticker_price.merge(df_xtb_open, on=['merge_key'], how='left')
    df = df.merge(df_xtb_closed, on=['merge_key'], how='left')
    df = df.merge(df_xtb_deposit, on=['merge_key'], how='left')
    df = df.merge(df_xtb_dividend, on=['merge_key'], how='left')

    # Create a new column 'open_value' and 'closed_value'
    df['open_shares_cum'] = df.groupby('symbol')['open_shares_cum'].fillna(method='ffill')
    df['buy_value_cum'] = df.groupby('symbol')['buy_value_cum'].fillna(method='ffill')

    df['open_value'] = df['open_shares_cum'] * df['open']

    df['odd'] = df['open_value'] - df['buy_value_cum']

    df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')

    df.to_excel(r'finance_portfel\output\stock_data.xlsx', index=False)

    return None

if __name__ == '__main__':
    stock_etl()

