import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from utils.load import (
    prepare_dataframe_for_sql, save_to_csv, save_to_postgresql,
    save_to_google_sheets, load_data
)

class TestLoad(unittest.TestCase):
    
    def setUp(self):
        """Setup DataFrame untuk testing"""
        self.test_df = pd.DataFrame({
            "Title": ["T-Shirt", "Pants"],
            "Price_in_rupiah": [400000, 480000],
            "Rating": [4.5, 3.8],
            "Colors": [3, 2],
            "Size": ["M", "L"],
            "Gender": ["Men", "Women"],
            "timestamp": ["2023-06-01 12:00:00", "2023-06-01 12:00:00"]
        })
        
        # URL database yang sama dengan main.py
        self.db_url = "postgresql+psycopg2://developer:123@localhost:5432/fashion_db"
        self.spreadsheet_id = "1nMUvtPISHCbKIESChSOW9KXqZydIh88vmxjhOcC9yBI"
    
    def test_prepare_dataframe_for_sql(self):
        """Test prepare_dataframe_for_sql function"""
        # Tambahkan nilai NaN ke DataFrame
        df_with_nan = self.test_df.copy()
        df_with_nan.loc[1, 'Size'] = None
        
        result = prepare_dataframe_for_sql(df_with_nan)
        
        # Verifikasi
        self.assertFalse(result.isna().any().any())  # Tidak ada nilai NaN
        self.assertEqual(result.loc[1, 'Size'], '')  # NaN diubah menjadi string kosong
    
    @patch('pandas.DataFrame.to_csv')
    def test_save_to_csv_success(self, mock_to_csv):
        """Test save_to_csv function berhasil"""
        result = save_to_csv(self.test_df, "test.csv")
        
        # Verifikasi
        self.assertTrue(result)
        mock_to_csv.assert_called_once_with("test.csv", index=False)
    
    @patch('pandas.DataFrame.to_csv')
    def test_save_to_csv_error(self, mock_to_csv):
        """Test save_to_csv function gagal"""
        # Setup mock untuk menimbulkan exception
        mock_to_csv.side_effect = Exception("Test error")
        
        result = save_to_csv(self.test_df, "test.csv")
        
        # Verifikasi
        self.assertFalse(result)
        mock_to_csv.assert_called_once()
    
    @patch('utils.load.create_engine')
    @patch('pandas.DataFrame.to_sql')
    def test_save_to_postgresql_success(self, mock_to_sql, mock_create_engine):
        """Test save_to_postgresql function berhasil"""
        # Setup mock engine
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Panggil fungsi dengan URL database yang sama dengan main.py
        result = save_to_postgresql(self.test_df, self.db_url)
        
        # Verifikasi
        self.assertTrue(result)
        mock_create_engine.assert_called_once()
        mock_to_sql.assert_called_once()
    
    @patch('utils.load.create_engine')
    def test_save_to_postgresql_error(self, mock_create_engine):
        """Test save_to_postgresql function gagal"""
        # Setup mock untuk menimbulkan exception
        mock_create_engine.side_effect = Exception("Test error")
        
        # Panggil fungsi dengan URL database yang sama dengan main.py
        result = save_to_postgresql(self.test_df, self.db_url)
        
        # Verifikasi
        self.assertFalse(result)
        mock_create_engine.assert_called_once()
    
    @patch('utils.load.service_account.Credentials.from_service_account_file')
    @patch('utils.load.build')
    @patch('os.path.exists')
    def test_save_to_google_sheets_success(self, mock_exists, mock_build, mock_credentials):
        """Test save_to_google_sheets function berhasil"""
        # Setup mocks
        mock_exists.return_value = True
        
        # Setup mock service dan sheets API
        mock_service = MagicMock()
        mock_sheets = MagicMock()
        mock_values = MagicMock()
        mock_update = MagicMock()
        
        mock_build.return_value = mock_service
        mock_service.spreadsheets.return_value = mock_sheets
        mock_sheets.values.return_value = mock_values
        mock_values.update.return_value = mock_update
        mock_update.execute.return_value = {"updatedCells": 10}
        
        # Panggil fungsi dengan spreadsheet ID yang sama dengan main.py
        result = save_to_google_sheets(
            self.test_df, 
            "credentials.json", 
            self.spreadsheet_id
        )
        
        # Verifikasi
        self.assertTrue(result)
        mock_build.assert_called_once()
        mock_credentials.assert_called_once()
    
    @patch('utils.load.service_account.Credentials.from_service_account_file')
    @patch('os.path.exists')
    def test_save_to_google_sheets_error(self, mock_exists, mock_credentials):
        """Test save_to_google_sheets function gagal"""
        # Setup mocks
        mock_exists.return_value = True
        
        # Setup mock untuk menimbulkan exception
        mock_credentials.side_effect = Exception("Test error")
        
        # Panggil fungsi dengan spreadsheet ID yang sama dengan main.py
        result = save_to_google_sheets(
            self.test_df, 
            "credentials.json", 
            self.spreadsheet_id
        )
        
        # Verifikasi
        self.assertFalse(result)
        mock_credentials.assert_called_once()
    
    @patch('utils.load.save_to_csv')
    @patch('utils.load.save_to_postgresql')
    @patch('utils.load.save_to_google_sheets')
    @patch('os.path.exists')
    def test_load_data_all_success(self, mock_exists, mock_sheets, mock_postgres, mock_csv):
        """Test load_data function dengan semua repositori berhasil"""
        # Setup mocks
        mock_exists.return_value = True
        mock_csv.return_value = True
        mock_postgres.return_value = True
        mock_sheets.return_value = True
        
        # Panggil fungsi dengan URL database dan spreadsheet ID yang sama dengan main.py
        result = load_data(
            self.test_df,
            save_csv=True,
            save_postgres=True,
            save_gsheets=True,
            db_url=self.db_url,
            credentials_path="credentials.json",
            spreadsheet_id=self.spreadsheet_id
        )
        
        # Verifikasi
        self.assertTrue(result["csv"])
        self.assertTrue(result["postgres"])
        self.assertTrue(result["gsheets"])
        mock_csv.assert_called_once()
        mock_postgres.assert_called_once()
        mock_sheets.assert_called_once()
    
    def test_load_data_empty_df(self):
        """Test load_data function dengan DataFrame kosong"""
        result = load_data(
            pd.DataFrame(),
            save_csv=True,
            save_postgres=True,
            save_gsheets=True
        )
        
        # Verifikasi
        self.assertFalse(result["csv"])
        self.assertFalse(result["postgres"])
        self.assertFalse(result["gsheets"])
    
    @patch('utils.load.save_to_csv')
    def test_load_data_error(self, mock_csv):
        """Test load_data function dengan error"""
        # Setup mock untuk menimbulkan exception
        mock_csv.side_effect = Exception("Test error")
        
        # Panggil fungsi
        result = load_data(
            self.test_df,
            save_csv=True,
            save_postgres=False,
            save_gsheets=False
        )
        
        # Verifikasi
        self.assertEqual(result, {"csv": False, "postgres": False, "gsheets": False})
        mock_csv.assert_called_once()

if __name__ == '__main__':
    unittest.main()