import pandas as pd
import re
import numpy as np

def clean_price(df):
    """
    Membersihkan dan mengonversi kolom Price dari USD ke Rupiah.
    
    Args:
        df (pd.DataFrame): DataFrame yang akan dibersihkan
        
    Returns:
        pd.DataFrame: DataFrame dengan kolom Price yang sudah dibersihkan
    """
    try:
        df_clean = df.copy()
        
        # Filter baris dengan harga yang valid (mengandung $)
        mask = df_clean['Price'].str.contains(r'\$', na=False)
        
        # Ekstrak angka dari string harga dan konversi ke Rupiah
        df_clean.loc[mask, 'Price_in_rupiah'] = df_clean.loc[mask, 'Price'].str.extract(r'\$(\d+\.?\d*)')[0].astype(float) * 16000
        
        # Baris dengan harga tidak valid akan diberi NaN
        df_clean.loc[~mask, 'Price_in_rupiah'] = float('nan')
        
        return df_clean
    
    except Exception as e:
        print(f"Error saat membersihkan kolom Price: {e}")
        raise

def clean_rating(df):
    """
    Membersihkan kolom Rating menjadi nilai float.
    
    Args:
        df (pd.DataFrame): DataFrame yang akan dibersihkan
        
    Returns:
        pd.DataFrame: DataFrame dengan kolom Rating yang sudah dibersihkan
    """
    try:
        df_clean = df.copy()
        
        # Ekstrak nilai numerik dari rating
        df_clean['Rating'] = df_clean['Rating'].apply(
            lambda x: float(re.search(r'([\d.]+)', str(x)).group(1)) 
            if isinstance(x, str) and re.search(r'([\d.]+)', str(x)) 
            else float('nan')
        )
        
        return df_clean
    
    except Exception as e:
        print(f"Error saat membersihkan kolom Rating: {e}")
        raise

def clean_colors(df):
    """
    Membersihkan kolom Colors menjadi nilai numerik.
    
    Args:
        df (pd.DataFrame): DataFrame yang akan dibersihkan
        
    Returns:
        pd.DataFrame: DataFrame dengan kolom Colors yang sudah dibersihkan
    """
    try:
        df_clean = df.copy()
        
        # Ekstrak angka dari string "X Colors"
        df_clean['Colors'] = df_clean['Colors'].apply(
            lambda x: int(re.search(r'(\d+)', str(x)).group(1)) 
            if isinstance(x, str) and re.search(r'(\d+)', str(x)) 
            else float('nan')
        )
        
        return df_clean
    
    except Exception as e:
        print(f"Error saat membersihkan kolom Colors: {e}")
        raise

def clean_size(df):
    """
    Membersihkan kolom Size dengan menghapus "Size: ".
    
    Args:
        df (pd.DataFrame): DataFrame yang akan dibersihkan
        
    Returns:
        pd.DataFrame: DataFrame dengan kolom Size yang sudah dibersihkan
    """
    try:
        df_clean = df.copy()
        
        # Hapus "Size: " dari string
        df_clean['Size'] = df_clean['Size'].apply(
            lambda x: str(x).replace('Size:', '').strip() 
            if isinstance(x, str) 
            else None
        )
        
        return df_clean
    
    except Exception as e:
        print(f"Error saat membersihkan kolom Size: {e}")
        raise

def clean_gender(df):
    """
    Membersihkan kolom Gender dengan menghapus "Gender: ".
    
    Args:
        df (pd.DataFrame): DataFrame yang akan dibersihkan
        
    Returns:
        pd.DataFrame: DataFrame dengan kolom Gender yang sudah dibersihkan
    """
    try:
        df_clean = df.copy()
        
        # Hapus "Gender: " dari string
        df_clean['Gender'] = df_clean['Gender'].apply(
            lambda x: str(x).replace('Gender:', '').strip() 
            if isinstance(x, str) 
            else None
        )
        
        return df_clean
    
    except Exception as e:
        print(f"Error saat membersihkan kolom Gender: {e}")
        raise

def remove_invalid_data(df):
    """
    Menghapus data yang tidak valid dari DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame yang akan dibersihkan
        
    Returns:
        pd.DataFrame: DataFrame tanpa data yang tidak valid
    """
    try:
        df_clean = df.copy()
        
        # Hapus baris dengan title "Unknown Product"
        df_clean = df_clean[df_clean['Title'] != "Unknown Product"]
        
        # Hapus baris dengan nilai NaN
        df_clean = df_clean.dropna()
        
        # Hapus duplikat
        df_clean = df_clean.drop_duplicates()
        
        return df_clean
    
    except Exception as e:
        print(f"Error saat menghapus data tidak valid: {e}")
        raise

def convert_data_types(df):
    """
    Mengonversi tipe data kolom-kolom dalam DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame yang akan dikonversi tipe datanya
        
    Returns:
        pd.DataFrame: DataFrame dengan tipe data yang sudah dikonversi
    """
    try:
        df_clean = df.copy()
        
        # Konversi tipe data
        df_clean['Title'] = df_clean['Title'].astype('string')
        df_clean['Price_in_rupiah'] = df_clean['Price_in_rupiah'].astype(float)
        df_clean['Rating'] = df_clean['Rating'].astype(float)
        df_clean['Colors'] = df_clean['Colors'].astype(int)
        df_clean['Size'] = df_clean['Size'].astype('string')
        df_clean['Gender'] = df_clean['Gender'].astype('string')
        df_clean['timestamp'] = df_clean['timestamp'].astype('string')
        
        return df_clean
    
    except Exception as e:
        print(f"Error saat mengonversi tipe data: {e}")
        raise

def transform_data(df):
    """
    Melakukan seluruh transformasi data.
    
    Args:
        df (pd.DataFrame): DataFrame yang akan ditransformasi
        
    Returns:
        pd.DataFrame: DataFrame yang sudah ditransformasi
    """
    try:
        if df is None or df.empty:
            print("DataFrame kosong atau None, tidak dapat melakukan transformasi")
            return None
        
        print("Memulai transformasi data...")
        
        # Terapkan semua fungsi transformasi
        df = clean_price(df)
        df = clean_rating(df)
        df = clean_colors(df)
        df = clean_size(df)
        df = clean_gender(df)
        df = remove_invalid_data(df)
        
        # Hapus kolom Price asli karena sudah ada Price_in_rupiah
        df = df.drop(columns=['Price'])
        
        # Konversi tipe data
        df = convert_data_types(df)
        
        print(f"Transformasi berhasil, jumlah data: {len(df)}")
        return df
    
    except Exception as e:
        print(f"Error pada proses transformasi: {e}")
        return None

if __name__ == "__main__":
    # Test dengan DataFrame dummy
    data = {
        "Title": ["T-Shirt", "Unknown Product", "Pants"],
        "Price": ["$25.99", "Price Unavailable", "$30"],
        "Rating": ["4.5 / 5", "Invalid Rating", "3.8 / 5"],
        "Colors": ["3 Colors", None, "2 Colors"],
        "Size": ["Size: M", None, "Size: L"],
        "Gender": ["Gender: Men", None, "Gender: Women"],
        "timestamp": ["2023-06-01 12:00:00", "2023-06-01 12:00:00", "2023-06-01 12:00:00"]
    }
    
    df = pd.DataFrame(data)
    transformed_df = transform_data(df)
    print(transformed_df)