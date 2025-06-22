from utils.extract import main as extract_main
from utils.transform import transform_data
from utils.load import load_data
import os
import pandas as pd

def main():
    """
    Fungsi utama untuk menjalankan ETL pipeline.
    """
    print("=== Fashion Studio ETL Pipeline ===")
    
    try:
        # Ekstraksi data
        print("\n=== Proses Ekstraksi Data ===")
        raw_df = extract_main()
        
        if raw_df is None or raw_df.empty:
            print("Ekstraksi data gagal, tidak ada data yang diperoleh")
            return
        
        print(f"Berhasil mengekstrak {len(raw_df)} produk")
        
        # Transformasi data
        print("\n=== Proses Transformasi Data ===")
        transformed_df = transform_data(raw_df)
        
        if transformed_df is None or transformed_df.empty:
            print("Transformasi data gagal")
            return
        
        print(f"Data setelah transformasi: {len(transformed_df)} produk")
        
        # Load data
        print("\n=== Proses Penyimpanan Data ===")
        
        db_url = "postgresql+psycopg2://developer:123@localhost:5432/fashion_db"
        credentials_path = "google-sheets-api.json"
        spreadsheet_id = "1nMUvtPISHCbKIESChSOW9KXqZydIh88vmxjhOcC9yBI"  
        
        # Cek apakah file kredensial ada
        has_credentials = os.path.exists(credentials_path)
        
        # Tampilkan informasi tentang DataFrame sebelum penyimpanan
        print(f"Info DataFrame sebelum penyimpanan:")
        print(f"- Jumlah baris: {len(transformed_df)}")
        print(f"- Kolom: {transformed_df.columns.tolist()}")
        print(f"- Tipe data:")
        for col, dtype in transformed_df.dtypes.items():
            print(f"  - {col}: {dtype}")
        
        # Periksa nilai NaN atau null
        null_counts = transformed_df.isna().sum()
        if null_counts.any():
            print("\nKolom dengan nilai NaN:")
            for col, count in null_counts.items():
                if count > 0:
                    print(f"  - {col}: {count} nilai NaN")
        
        # Simpan data
        load_result = load_data(
            df=transformed_df,
            save_csv=True,
            save_postgres=True,
            save_gsheets=has_credentials,
            db_url=db_url,
            credentials_path=credentials_path,
            spreadsheet_id=spreadsheet_id
        )
        
        # Tampilkan hasil
        print("\n=== Hasil ETL Pipeline ===")
        print(f"CSV: {'Berhasil' if load_result.get('csv') else 'Gagal'}")
        print(f"PostgreSQL: {'Berhasil' if load_result.get('postgres') else 'Gagal'}")
        print(f"Google Sheets: {'Berhasil' if load_result.get('gsheets') else 'Gagal'}")
        
        print("\n=== ETL Pipeline Selesai ===")
        
    except Exception as e:
        print(f"Error pada ETL pipeline: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()