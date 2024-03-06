'''
Developer: Tomasz Dudzik

Desc: Module to get ETL bank main data
'''
import sys
sys.path.append('finance_portfel/')
from data_source.bank_main_data import get_bank_main_data

def bank_etl():
    """
    Extracts, transforms, and loads bank data.

    Returns:
    dict: A dictionary containing two dataframes - 'transaction' and 'saldo'.
    """

    # Get the data
    df = get_bank_main_data()

    # Group by 'date', sum up 'value' and keep only the first 'saldo' value for each date
    df = df.groupby(['date', 'source', 'currency']).agg({'value': 'sum', 'saldo': 'first'}).reset_index()

    # Set the 'date' column as the index
    df.set_index(['date'], inplace=True)

    # Create transaction dataframe
    selected_columns = ['source', 'currency', 'value']
    df_transaction = df[selected_columns]

    # Create saldo dataframe
    selected_columns = ['source', 'currency', 'saldo']
    df_saldo = df[selected_columns]

    # Fill forward the missing values
    df_saldo= df_saldo.groupby(['source','currency']).resample('D').ffill()

    # List of columns to drop, reset the index and set the 'date' column as the index
    cols_to_drop = ['source','currency']
    df_saldo = df_saldo.drop(columns=cols_to_drop)
    df_saldo.reset_index(inplace=True)
    df_saldo.set_index('date', inplace=True)

    # Create a dictionary with the two dataframes
    result = {'transaction': df_transaction, 'saldo': df_saldo}

    return result

if __name__ == '__main__':
    bank_etl()
