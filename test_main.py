import pandas as pd
import os
import unittest
from unittest.mock import patch
from main import main

class TestMain(unittest.TestCase):
    @patch('main.bank_etl')
    def test_main(self, mock_bank_etl):
        # Mock the bank_etl function to return a sample bank_data dictionary
        mock_bank_etl.return_value = {
            'data1': pd.DataFrame({'col1': [1, 2, 3]}),
            'data2': pd.DataFrame({'col2': [4, 5, 6]})
        }

        # Call the main function
        main()

        # Assert that the expected csv files are created
        expected_files = ['data1.csv', 'data2.csv']
        for file in expected_files:
            self.assertTrue(os.path.exists(f'finance_portfel/output/{file}'))

if __name__ == '__main__':
    unittest.main()