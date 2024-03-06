'''
Developer: Tomasz Dudzik
Desc: module to get mbank main data from csv extract file stored on local drive
Note: need to change to some sharepoint site
'''
import os
import glob
import pandas as pd

# Get the path of the folder containing the CSV file
folder_path = r'C:\Users\dudzi\Desktop\Portfel\raw_data'
# Get the folder name from the file path
folder_name = os.path.basename(folder_path)

def get_main_data():
    """
    get_main_data function

    This function processes bank statement data from various sources. 
    It reads CSV files from a specified folder, renames and formats the columns, and appends the data to a final DataFrame. 
    The function handles different formats for different banks, including 'santander', 'ing', and 'mbank'. 
    The final DataFrame is saved to an Excel file.

    Parameters:
    folder_path (str): The path to the folder containing the CSV files. 
    The function processes all CSV files in this folder and its subfolders. 
    This variable is not defined within the function and should be defined in the scope where this function is called.

    Returns:
    None. The function saves the final DataFrame to an Excel file and does not return a value.

    Raises:
    Prints "Folder does not exist" if the specified folder does not exist.
    """
    # Check if the folder exists
    if os.path.exists(folder_path):
        # Create an empty dataframe to store the data
        df_all = pd.DataFrame()

        # Get a list of all the CSV files in the folder
        csv_files = glob.glob(os.path.join(folder_path, '**/*.csv'))

        # Loop through each CSV file and load it using pandas
        for csv_file in csv_files:
            df = pd.read_csv(csv_file, 
                             encoding='ISO-8859-1', 
                             error_bad_lines=False, 
                             sep='~~', 
                             header=None)
            
            # Get the folder name from the file path
            folder_name = os.path.basename(os.path.dirname(csv_file))

            # Configure the columns to be selected based on the folder name
            if 'ing' in folder_name:
                first_four = df[0].apply(lambda x: str(x)[:4])
                selected_columns = [0, 2, 8, 9, 15, 16]
            elif 'mbank' in folder_name:
                first_four = df[0].apply(lambda x: str(x)[:4])
                selected_columns = [0, 1, 4, 5]
            elif 'santander' in folder_name:
                first_four = df[0].apply(lambda x: str(x)[6:10])
                selected_columns  = [0, 2, 5, 6]

            # Check if the first four characters are digits
            mask = first_four.str.isdigit()

            # Select only the rows where the first four characters are digits
            df = df[mask]

            # Split the values in the DataFrame by ';'
            df = df[0].str.split(';', expand=True)

            # Select only needed columns
            df = df[selected_columns]

            # Replace empty strings with NaN values
            df.replace("", None, inplace=True)

            # Configure the columns to be selected based on the folder name
            if 'ing' in folder_name:
                # Rename columns
                df = df.rename(columns={0:  'date',
                                        2:  'title',
                                        8:  'value',
                                        9:  'currency',
                                        15: 'saldo'
                                        })
                df['source'] = folder_name
                df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

            elif 'mbank' in folder_name:
                # Create a new column that takes the last 3 characters of the original column
                df['currency'] = df[4].str[-3:]
                # Remove the last 3 characters from the column
                df['value'] = df[4].str[:-3]
                # Remove the last 3 characters from the column
                df['saldo'] = df[5].str[:-3]
                # Rename columns
                df = df.rename(columns={0:  'date',
                                        1:  'title'
                                        })
                # Remove unnecessary columns
                df.drop([4, 5], axis=1, inplace=True)
                df['source'] = folder_name
                df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

            elif 'santander' in folder_name:
                # Rename columns
                df = df.rename(columns={0:  'date',
                                        2:  'title',
                                        5:  'value',
                                        6:  'saldo'
                                        })
                df['currency'] = 'PLN'
                df['source'] = folder_name
                df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')

            # Replace ',' with '.' and change type of columns
            df['value'] = df['value'].str.replace(',', '.')
            df['value'] = df['value'].str.replace(' ', '').astype(float)
            df['saldo'] = df['saldo'].str.replace(',', '.')
            df['saldo'] = df['saldo'].str.replace(' ', '').astype(float)
            df['date'] = df['date'].astype('datetime64[ns]')
 
            # Remove rows with missing values
            df.dropna(axis=0, inplace=True)

            # Append the data to the final dataframe
            df_all = df_all.append(df)

            # Drop duplicates
            df_all.drop_duplicates(inplace=True)

    else:
        print("Folder does not exist")

    # Group by 'date', sum up 'value' and keep only the first 'saldo' value for each date
    df_all = df_all.groupby(['date', 'source', 'currency']).agg({'value': 'sum', 'saldo': 'first'}).reset_index()

    # Set the 'date' column as the index
    df_all.set_index(['date'], inplace=True)

    # Create transaction dataframe
    selected_columns = ['source', 'currency', 'value']
    df_transaction = df_all[selected_columns]

    # Create balance dataframe
    selected_columns = ['source', 'currency', 'saldo']
    df_balance = df_all[selected_columns]

    # Fill forward the missing values
    df_balance= df_balance.groupby(['source','currency']).resample('D').ffill()

    # List of columns to drop, reset the index and set the 'date' column as the index
    cols_to_drop = ['source','currency']
    df_balance = df_balance.drop(columns=cols_to_drop)
    df_balance.reset_index(inplace=True)
    df_balance.set_index('date', inplace=True)

    # Create a dictionary with the two dataframes
    result = {'transaction': df_transaction, 'balance': df_balance}

    # Save the data to an Excel file
    result.to_excel(r'C:\Users\dudzi\Desktop\Portfel\raw_data\bank_statments.xlsx')

    return result

if __name__ == '__main__':
    get_main_data()
