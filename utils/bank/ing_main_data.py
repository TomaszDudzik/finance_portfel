'''
Developer: Tomasz Dudzik
Desc: module to get mbank main data from csv extract file stored on local drive
Note: need to change to some sharepoint site
'''
import os
import glob
import pandas as pd

# Get the path of the folder containing the CSV file
folder_path = r'C:\Users\dudzi\Desktop\Portfel\raw_data\ing'

def get_main_data():
    '''Function to get main data from csv file'''
    # Check if the folder exists
    if os.path.exists(folder_path):
        # Create an empty dataframe to store the data
        result = pd.DataFrame()

        # Get a list of all the CSV files in the folder
        csv_files = glob.glob(folder_path + '/*.csv')

        # Loop through each CSV file and load it using pandas
        for csv_file in csv_files:
            # Define the expected columns
            expected_columns = [0, 1]
            df = pd.read_csv(csv_file, 
                             encoding='ISO-8859-1', 
                             error_bad_lines=False, 
                             sep='~~', 
                             header=None)

            # Extract the first four characters of each row as a string.
            # This is to find the first row contains the date, everhitng before is not needed.
            first_four = df[0].apply(lambda x: str(x)[:4])

            # Check if the first four characters are digits
            mask = first_four.str.isdigit()

            # Select only the rows where the first four characters are digits
            df = df[mask]

            # Split the values in the DataFrame by ';'
            df = df[0].str.split(';', expand=True)

            # Select only needed columns
            df = df[[0, 2, 8, 9, 15, 16]]

            # Replace empty strings with NaN values
            df.replace("", None, inplace=True)

            # Remove rows with missing values
            df.dropna(axis=0, inplace=True)

            # Rename columns
            df = df.rename(columns={0:  'date',
                                    2:  'title',
                                    8:  'value',
                                    9:  'value_currency',
                                    15: 'saldo',
                                    16: 'saldo_currency'
                                    })

            # Replace ',' with '.' and change type of columns
            df['value'] = df['value'].str.replace(',', '.').astype(float)
            df['saldo'] = df['saldo'].str.replace(',', '.').astype(float)
            df['date'] = df['date'].astype('datetime64[ns]')

            # Append the data to the final dataframe
            result = result.append(df)

    else:
        print("Folder does not exist")

    return result

if __name__ == '__main__':
    get_main_data()
