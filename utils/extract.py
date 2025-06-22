import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

# Header untuk menghindari pemblokiran
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
    )
}

def fetching_content(url):
    """Mengambil konten HTML dari URL yang diberikan."""
    try:
        session = requests.Session()
        response = session.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        print(f"Terjadi kesalahan ketika melakukan requests terhadap {url}: {e}")
        return None

def extract_product_data(card):
    """
    Mengambil data produk fashion dari elemen HTML.
    
    Args:
        card (BeautifulSoup element): Elemen HTML dengan class 'collection-card'
        
    Returns:
        dict: Dictionary berisi data produk atau None jika terjadi error
    """
    try:
        # Berdasarkan struktur HTML yang diberikan
        
        # Ekstrak judul produk
        title_element = card.find('h3', class_='product-title')
        title = title_element.text.strip() if title_element else "Unknown Product"
        
        # Ekstrak harga
        price_element = card.find('span', class_='price')
        price = price_element.text.strip() if price_element else "Price Unavailable"
        
        # Ekstrak rating
        rating_element = card.find('p', string=lambda text: text and 'Rating:' in text)
        if rating_element:
            rating = rating_element.text.strip()
        else:
            rating = "Invalid Rating"
        
        # Ekstrak warna
        colors_element = card.find('p', string=lambda text: text and 'Colors:' in text)
        colors = colors_element.text.strip() if colors_element else "3 Colors"
        
        # Ekstrak ukuran
        size_element = card.find('p', string=lambda text: text and 'Size:' in text)
        size = size_element.text.strip() if size_element else "Size: M"
        
        # Ekstrak gender
        gender_element = card.find('p', string=lambda text: text and 'Gender:' in text)
        gender = gender_element.text.strip() if gender_element else "Gender: Unisex"
        
        # Tambahkan timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return {
            "Title": title,
            "Price": price,
            "Rating": rating,
            "Colors": colors,
            "Size": size,
            "Gender": gender,
            "timestamp": timestamp
        }
    except Exception as e:
        print(f"Error saat mengekstrak data produk: {e}")
        return None

def scrape_fashion_products(base_url, max_pages=50, delay=2):
    """
    Fungsi utama untuk mengambil data produk fashion dari semua halaman.
    
    Args:
        base_url (str): URL dasar website
        max_pages (int): Jumlah maksimum halaman yang akan di-scrape
        delay (int): Delay antar request dalam detik
        
    Returns:
        list: List berisi data semua produk
    """
    data = []
    
    # Buat daftar URL untuk semua halaman
    urls = [base_url]  # Halaman 1
    
    # URL untuk halaman 2 sampai max_pages
    for page in range(2, max_pages + 1):
        urls.append(f"{base_url}/page{page}")
    
    for page_number, url in enumerate(urls, 1):
        print(f"Scraping halaman {page_number}: {url}")
        
        # Ambil konten halaman
        content = fetching_content(url)
        if not content:
            print(f"Gagal mengambil konten halaman {page_number}")
            if page_number > 1:  # Jangan berhenti di halaman pertama
                print("Mungkin sudah mencapai halaman terakhir")
                break
            continue
        
        # Parse HTML
        soup = BeautifulSoup(content, "html.parser")
        
        # Gunakan selector yang benar: collection-card
        collection_cards = soup.find_all('div', class_='collection-card')
        
        if not collection_cards:
            print(f"Tidak ada produk ditemukan di halaman {page_number}")
            if page_number > 1:  # Jangan berhenti di halaman pertama
                print("Kemungkinan sudah mencapai halaman terakhir")
                break
            continue
        
        print(f"Ditemukan {len(collection_cards)} produk di halaman {page_number}")
        
        # Ekstrak data dari setiap produk
        for card in collection_cards:
            product_data = extract_product_data(card)
            if product_data:
                data.append(product_data)
        
        # Delay untuk menghindari overload server
        if page_number < len(urls):
            time.sleep(delay)
    
    return data

def main():
    """
    Fungsi utama untuk menjalankan proses ekstraksi data.
    
    Returns:
        pd.DataFrame: DataFrame berisi data produk fashion
    """
    try:
        base_url = "https://fashion-studio.dicoding.dev"
        products = scrape_fashion_products(base_url, max_pages=50)
        
        if not products:
            print("Tidak ada data yang berhasil diekstrak")
            return None
        
        df = pd.DataFrame(products)
        print(f"Berhasil mengekstrak {len(df)} produk")
        return df
    
    except Exception as e:
        print(f"Error pada proses ekstraksi: {e}")
        return None

if __name__ == "__main__":
    main()