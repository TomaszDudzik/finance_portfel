'''
Main file to run the ETL process.
'''
# Get imports
from etl.bank_etl import bank_etl

def main():
    """
    Main function to run the ETL process.
    """
    # Get the data
    bank_data = bank_etl()

    # Save all dataframe in dictionary to separate csv files
    for key in bank_data.keys():
        bank_data[key].to_csv(f'finance_portfel/output/{key}.csv', sep='~', index=True)
        print(f'{key} saved to csv')

if __name__ == '__main__':
    main()
