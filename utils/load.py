import pandas as pd
import os
from sqlalchemy import create_engine, text
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def prepare_dataframe_for_sql(df):
    """
    Mempersiapkan DataFrame untuk disimpan ke SQL.
    
    Args:
        df (pd.DataFrame): DataFrame yang akan dipersiapkan
        
    Returns:
        pd.DataFrame: DataFrame yang sudah dipersiapkan
    """
    try:
        df_copy = df.copy()
        
        # Tangani NaN dengan lebih baik
        df_copy = df_copy.fillna('')
        
        # Konversi tipe data yang mungkin bermasalah dengan lebih spesifik
        for col in df_copy.columns:
            # Konversi objek ke string
            if df_copy[col].dtype == 'object':
                df_copy[col] = df_copy[col].astype(str)
            
            # Pastikan kolom numerik tetap numerik
            elif pd.api.types.is_numeric_dtype(df_copy[col]):
                # Konversi NaN ke 0 untuk kolom numerik
                df_copy[col] = df_copy[col].fillna(0)
        
        # Hapus karakter yang mungkin menyebabkan masalah SQL
        for col in df_copy.select_dtypes(['object']).columns:
            df_copy[col] = df_copy[col].str.replace('\x00', '')  # Hapus null bytes
        
        return df_copy
    except Exception as e:
        print(f"Error saat mempersiapkan DataFrame: {e}")
        import traceback
        traceback.print_exc()
        return df

def save_to_csv(df, file_path="products.csv"):
    """
    Menyimpan DataFrame ke file CSV.
    
    Args:
        df (pd.DataFrame): DataFrame yang akan disimpan
        file_path (str): Path file CSV tujuan
        
    Returns:
        bool: True jika berhasil, False jika gagal
    """
    try:
        df.to_csv(file_path, index=False)
        print(f"Data berhasil disimpan ke {file_path}")
        return True
    
    except Exception as e:
        print(f"Error saat menyimpan ke CSV: {e}")
        import traceback
        traceback.print_exc()
        return False

def save_to_postgresql(df, db_url):
    """
    Menyimpan DataFrame ke database PostgreSQL.
    
    Args:
        df (pd.DataFrame): DataFrame yang akan disimpan
        db_url (str): URL koneksi database PostgreSQL
        
    Returns:
        bool: True jika berhasil, False jika gagal
    """
    try:
        print(f"Mencoba koneksi ke database dengan URL: {db_url}")
        
        # Tambahkan parameter echo=True untuk debugging
        engine = create_engine(db_url, echo=True)
        
        # Persiapkan DataFrame untuk SQL
        df_prepared = prepare_dataframe_for_sql(df)
        
        print("Koneksi database berhasil, mencoba menyimpan data...")
        
        try:
            # Tentukan tipe data untuk kolom objek
            dtype_dict = {col: 'TEXT' for col in df_prepared.select_dtypes(['object']).columns}
            
            # Simpan data ke tabel - TANPA parameter schema
            df_prepared.to_sql(
                'fashion_products', 
                con=engine, 
                if_exists='replace', 
                index=False,
                dtype=dtype_dict
                # Hapus parameter schema=xxx
            )
            
            print("Data berhasil disimpan ke PostgreSQL dalam schema public")
            return True
            
        except Exception as e:
            print(f"Error saat menyimpan data: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
            
    except Exception as e:
        print(f"Error saat membuat koneksi database: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def save_to_google_sheets(df, credentials_path, spreadsheet_id):
    """
    Menyimpan DataFrame ke Google Sheets.
    
    Args:
        df (pd.DataFrame): DataFrame yang akan disimpan
        credentials_path (str): Path ke file credentials Google Sheets API
        spreadsheet_id (str): ID spreadsheet Google Sheets
        
    Returns:
        bool: True jika berhasil, False jika gagal
    """
    try:
        # Memuat kredensial
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path, 
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        
        # Membuat service
        service = build('sheets', 'v4', credentials=credentials)
        
        # Konversi DataFrame ke list values
        values = [df.columns.tolist()]  # Header
        values.extend(df.values.tolist())  # Data
        
        # Mengirim data ke Google Sheets
        body = {
            'values': values
        }
        
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range='Sheet1!A1',
            valueInputOption='RAW',
            body=body
        ).execute()
        
        print(f"Data berhasil disimpan ke Google Sheets, {result.get('updatedCells')} sel diperbarui")
        return True
    
    except HttpError as e:
        print(f"Error API Google Sheets: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    except Exception as e:
        print(f"Error saat menyimpan ke Google Sheets: {e}")
        import traceback
        traceback.print_exc()
        return False

def load_data(df, save_csv=True, save_postgres=False, save_gsheets=False, 
              db_url=None, credentials_path=None, spreadsheet_id=None):
    """
    Menyimpan data ke berbagai repositori.
    
    Args:
        df (pd.DataFrame): DataFrame yang akan disimpan
        save_csv (bool): Flag untuk menyimpan ke CSV
        save_postgres (bool): Flag untuk menyimpan ke PostgreSQL
        save_gsheets (bool): Flag untuk menyimpan ke Google Sheets
        db_url (str): URL koneksi database PostgreSQL
        credentials_path (str): Path ke file credentials Google Sheets API
        spreadsheet_id (str): ID spreadsheet Google Sheets
        
    Returns:
        dict: Status penyimpanan untuk setiap repositori
    """
    try:
        if df is None or df.empty:
            print("DataFrame kosong atau None, tidak dapat melakukan penyimpanan")
            return {"csv": False, "postgres": False, "gsheets": False}
        
        print("Memulai proses penyimpanan data...")
        result = {}
        
        # Simpan ke CSV
        if save_csv:
            result["csv"] = save_to_csv(df)
        else:
            result["csv"] = False
        
        # Simpan ke PostgreSQL
        if save_postgres and db_url:
            print(f"Menyimpan ke PostgreSQL dengan URL: {db_url}")
            result["postgres"] = save_to_postgresql(df, db_url)
        else:
            result["postgres"] = False
            if save_postgres and not db_url:
                print("URL database tidak disediakan untuk PostgreSQL")
        
        # Simpan ke Google Sheets
        if save_gsheets and credentials_path and spreadsheet_id:
            if os.path.exists(credentials_path):
                result["gsheets"] = save_to_google_sheets(df, credentials_path, spreadsheet_id)
            else:
                print(f"File kredensial {credentials_path} tidak ditemukan")
                result["gsheets"] = False
        else:
            result["gsheets"] = False
            if save_gsheets:
                if not credentials_path:
                    print("Path kredensial tidak disediakan untuk Google Sheets")
                if not spreadsheet_id:
                    print("ID spreadsheet tidak disediakan untuk Google Sheets")
        
        return result
    
    except Exception as e:
        print(f"Error pada proses penyimpanan data: {e}")
        import traceback
        traceback.print_exc()
        return {"csv": False, "postgres": False, "gsheets": False}

if __name__ == "__main__":
    # Test dengan DataFrame dummy
    data = {
        "Title": ["T-Shirt", "Pants"],
        "Price_in_rupiah": [400000, 480000],
        "Rating": [4.5, 3.8],
        "Colors": [3, 2],
        "Size": ["M", "L"],
        "Gender": ["Men", "Women"],
        "timestamp": ["2023-06-01 12:00:00", "2023-06-01 12:00:00"]
    }
    
    df = pd.DataFrame(data)
    load_data(df, save_csv=True)