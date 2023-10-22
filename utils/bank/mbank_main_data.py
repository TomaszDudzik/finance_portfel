'''
Developer: Tomasz Dudzik
Desc: module to get mbank main data from csv extract file stored on local drive
Note: need to change to some sharepoint site
'''
import os
import glob
import pandas as pd

# Get the path of the folder containing the CSV file
folder_path = r'C:\Users\dudzi\Desktop\Portfel\raw_data\mbank'

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
            df = pd.read_csv(csv_file, encoding='ISO-8859-1', error_bad_lines=False, sep=';', header=None)

            # Extract the first four characters of each row as a string. 
            # This is to find the first row contains the date, everhitng before is not needed
            first_four = df[0].apply(lambda x: str(x)[:4])

            # Check if the first four characters are digits
            mask = first_four.str.isdigit()

            # Select only the rows where the first four characters are digits
            df = df[mask]

            # Select only needed columns
            df = df[[0, 2, 8, 9, 15, 16]]

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

import pandas as pd
import os

def test_get_main_data():
    # Create a temporary CSV file for testing
    test_data = 'date,title,value,value_currency,saldo,saldo_currency\n2022-01-01,Test,1.23,PLN,4.56,PLN\n'
    with open('test_data.csv', 'w') as f:
        f.write(test_data)

    # Test the function with the temporary CSV file
    result = get_main_data()

    # Check that the result is a DataFrame
    assert isinstance(result, pd.DataFrame)

    # Check that the result has the correct columns
    expected_columns = ['date', 'title', 'value', 'value_currency', 'saldo', 'saldo_currency']
    assert result.columns.tolist() == expected_columns

    # Check that the result has the correct values
    expected_values = pd.DataFrame({'date': ['2022-01-01'], 'title': ['Test'], 'value': [1.23], 'value_currency': ['PLN'], 'saldo': [4.56], 'saldo_currency': ['PLN']})
    pd.testing.assert_frame_equal(result, expected_values)

    # Remove the temporary CSV file
    os.remove('test_data.csv')

if __name__ == '__main__':
    test_get_main_data()
