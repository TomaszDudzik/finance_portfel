'''
Developer: Tomasz Dudzik

Desc: module to get xtb main data from excel extract file stored on local drive
Note: need to change to some sharepoint site
'''
import os
import glob
import yaml
import numpy as np
import pandas as pd
from openpyxl import load_workbook

def df_cash_etl(df, sheet_name):
    # Find the row containing 'Waluta'
    row, col = np.where(df == 'Waluta')
    currency = df.iloc[row[0]+1, col[0]]

    # Find the index of the first row containing 'ID'
    id_index = df[df.isin(['ID']).any(axis=1)].index[0]

    # Set the row at 'ID' index as the header and Select all rows from 'ID' to the end 
    df.columns = df.iloc[id_index]
    df = df.loc[id_index + 1:]

    # Select only the columns with the relevant data
    df = df.iloc[:, [2, 3, 5, 6]]

    # Rename columns by index
    df.columns.values[0] = 'type'
    df.columns.values[1] = 'date'
    df.columns.values[2] = 'symbol'
    df.columns.values[3] = 'value'
    df['source'] = 'xtb_' + sheet_name.lower().replace(' ', '_')
    df['currency'] = currency
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

    # Split the dataframe into deposit and divident
    df_deposit = df.loc[df['type'] == 'Wplata']
    df_divident = df.loc[df['type'] == 'Dywidenda']

    # Create a dictionary to store the dataframes
    result = {'deposit': df_deposit, 'divident': df_divident}

    return result

def df_open_etl(df, sheet_name):
    # Find the row containing 'Waluta'
    row, col = np.where(df == 'Waluta')
    currency = df.iloc[row[0]+1, col[0]]

    # Find the index of the first row containing 'ID'
    id_index = df[df.isin(['Pozycja']).any(axis=1)].index[0]

    # Set the row at 'ID' index as the header and Select all rows from 'ID' to the end 
    df.columns = df.iloc[id_index]
    df = df.loc[id_index + 1:]

    # Select only the columns with the relevant data
    df = df.iloc[:, [2, 3, 4, 5, 6, 7, 8, 15]]

    # Rename columns by index
    df.columns.values[0] = 'symbol'
    df.columns.values[1] = 'type'
    df.columns.values[2] = 'shares'
    df.columns.values[3] = 'open_date'
    df.columns.values[4] = 'open_price'
    df.columns.values[5] = 'current_price'
    df.columns.values[6] = 'current_value [EUR]'
    df.columns.values[7] = 'profit_lose [EUR]'
    df['source'] = 'xtb_' + sheet_name.lower().replace(' ', '_')
    df['currency'] = currency
    df['category'] = 'open_position'
    df['open_date'] = pd.to_datetime(df['open_date'], format='%Y-%m-%d')

    # Split the dataframe into deposit and divident
    df = df.loc[df['type'].notnull()]

    # Create a dictionary to store the dataframes
    result = {'open': df}

    return result

def df_closed_etl(df, sheet_name):
    # Find the row containing 'Waluta'
    row, col = np.where(df == 'Waluta')
    currency = df.iloc[row[0]+1, col[0]]

    # Find the index of the first row containing 'ID'
    id_index = df[df.isin(['Pozycja']).any(axis=1)].index[0]

    # Set the row at 'ID' index as the header and Select all rows from 'ID' to the end 
    df.columns = df.iloc[id_index]
    df = df.loc[id_index + 1:]

    # Select only the columns with the relevant data
    df = df.iloc[:, [2, 3, 4, 5, 6, 7, 8, 11, 12, 19]]

    # Rename columns by index
    df.columns.values[0] = 'symbol'
    df.columns.values[1] = 'type'
    df.columns.values[2] = 'shares'
    df.columns.values[3] = 'open_date'
    df.columns.values[4] = 'open_price'
    df.columns.values[7] = 'open_value [EUR]'    
    df.columns.values[5] = 'closed_date'
    df.columns.values[6] = 'closed_price'
    df.columns.values[8] = 'closed_value [EUR]'
    df.columns.values[9] = 'profit_lose [EUR]'
    df['source'] = 'xtb_' + sheet_name.lower().replace(' ', '_')
    df['currency'] = currency
    df['category'] = 'closed_position'
    df['open_date'] = pd.to_datetime(df['open_date'], format='%Y-%m-%d')
    df['closed_date'] = pd.to_datetime(df['closed_date'], format='%Y-%m-%d')

    # Split the dataframe into deposit and divident
    df = df.loc[df['type'].notnull()]

    # Create a dictionary to store the dataframes
    result = {'closed': df}

    return result

def get_xtb_main_data():
    '''
    Function to get xtb main data from excel extract file stored on local drive
    '''
    # Load the configuration file
    with open('finance_portfel/variables.yaml', 'r') as file:
        config = yaml.safe_load(file)

    # Get the path of the folder containing the CSV file
    folder_path = config['data_source_path']['xtb_path']

    # Check if the folder exists
    if os.path.exists(folder_path):

        # Get a list of all the files in the folder
        files = os.listdir(folder_path)

        # Get the first file in the list
        xlsx_file = os.path.join(folder_path, files[0])

        # Load the workbook and get all sheet names
        wb = load_workbook(filename=xlsx_file, read_only=True)
        sheet_names = wb.sheetnames

        # Find the sheet name that contains the specific text
        sn_closed_position = next((sheet for sheet in sheet_names if sheet == 'CLOSED POSITION HISTORY'), None)
        sn_open_position = next((sheet for sheet in sheet_names if 'OPEN POSITION' in sheet), None)
        sn_cash_operation = next((sheet for sheet in sheet_names if 'CASH OPERATION' in sheet), None)

        # Load the data from the sheets
        df_closed = pd.read_excel(xlsx_file, sheet_name=sn_closed_position)
        df_open = pd.read_excel(xlsx_file, sheet_name=sn_open_position)
        df_cash = pd.read_excel(xlsx_file, sheet_name=sn_cash_operation)
        
        # ETL dataframes
        df_closed = df_closed_etl(df_closed, sn_closed_position)['closed']
        df_open = df_open_etl(df_open, sn_open_position)['open']
        df_open_closed = pd.concat([df_open, df_closed], axis=0)
        df_deposit = df_cash_etl(df_cash, sn_cash_operation)['deposit']
        df_divident = df_cash_etl(df_cash, sn_cash_operation)['divident']

        # Create a dictionary to store the dataframes
        result = {'open_closed': df_open_closed, 'deposit': df_deposit, 'divident': df_divident}

    return result

if __name__ == '__main__':
    get_xtb_main_data()

    