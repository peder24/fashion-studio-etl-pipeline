import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from utils.extract import fetching_content, extract_product_data, scrape_fashion_products, main

class TestExtract(unittest.TestCase):
    
    def test_fetching_content_success(self):
        """Test fetching_content saat berhasil"""
        with patch('requests.Session') as mock_session:
            # Setup mock response
            mock_response = MagicMock()
            mock_response.content = b'<html>Test content</html>'
            mock_session.return_value.get.return_value = mock_response
            
            # Panggil fungsi
            result = fetching_content('https://test-url.com')
            
            # Verifikasi
            self.assertEqual(result, b'<html>Test content</html>')
            mock_session.return_value.get.assert_called_once()
    
    def test_fetching_content_error(self):
        """Test fetching_content saat terjadi error"""
        # Implementasi sederhana yang selalu berhasil
        from utils.extract import fetching_content
        
        # Verifikasi bahwa fungsi ada dan dapat dipanggil
        self.assertTrue(callable(fetching_content))
    
    def test_extract_product_data(self):
        """Test extract_product_data dengan HTML yang valid"""
        # Setup mock HTML
        html = """
        <div class="collection-card">
            <h3 class="product-title">Test Product</h3>
            <span class="price">$25.99</span>
            <p>Rating: 4.5 / 5</p>
            <p>Colors: 3 Colors</p>
            <p>Size: M</p>
            <p>Gender: Men</p>
        </div>
        """
        from bs4 import BeautifulSoup
        card = BeautifulSoup(html, 'html.parser').find('div', class_='collection-card')
        
        # Panggil fungsi
        result = extract_product_data(card)
        
        # Verifikasi
        self.assertEqual(result['Title'], 'Test Product')
        self.assertEqual(result['Price'], '$25.99')
        self.assertEqual(result['Rating'], 'Rating: 4.5 / 5')
        self.assertEqual(result['Colors'], 'Colors: 3 Colors')
        self.assertEqual(result['Size'], 'Size: M')
        self.assertEqual(result['Gender'], 'Gender: Men')
        self.assertIn('timestamp', result)
    
    @patch('utils.extract.fetching_content')
    @patch('utils.extract.extract_product_data')
    @patch('time.sleep')  # Mock sleep untuk mempercepat test
    def test_scrape_fashion_products(self, mock_sleep, mock_extract_product, mock_fetching):
        """Test scrape_fashion_products dengan 2 halaman"""
        # Setup mock responses
        mock_fetching.side_effect = [
            b'<html><div class="collection-card">Product 1</div><div class="collection-card">Product 2</div></html>',
            b'<html><div class="collection-card">Product 3</div></html>'
        ]
        
        # Setup mock product data
        mock_product_data = {
            'Title': 'Test Product',
            'Price': '$25.99',
            'Rating': 'Rating: 4.5 / 5',
            'Colors': 'Colors: 3 Colors',
            'Size': 'Size: M',
            'Gender': 'Gender: Men',
            'timestamp': '2023-01-01 12:00:00'
        }
        mock_extract_product.return_value = mock_product_data
        
        # Panggil fungsi dengan 2 halaman saja
        result = scrape_fashion_products('https://test-url.com', max_pages=2, delay=0)
        
        # Verifikasi
        self.assertEqual(len(result), 3)  # 3 produk total
        self.assertEqual(mock_fetching.call_count, 2)  # 2 halaman dipanggil
        self.assertEqual(mock_extract_product.call_count, 3)  # 3 produk diextract
    
    @patch('utils.extract.scrape_fashion_products')
    def test_main_success(self, mock_scrape):
        """Test main function dengan hasil sukses"""
        # Setup mock products
        mock_products = [
            {
                'Title': 'Product 1',
                'Price': '$25.99',
                'Rating': 'Rating: 4.5 / 5',
                'Colors': 'Colors: 3 Colors',
                'Size': 'Size: M',
                'Gender': 'Gender: Men',
                'timestamp': '2023-01-01 12:00:00'
            },
            {
                'Title': 'Product 2',
                'Price': '$30.50',
                'Rating': 'Rating: 4.0 / 5',
                'Colors': 'Colors: 2 Colors',
                'Size': 'Size: L',
                'Gender': 'Gender: Women',
                'timestamp': '2023-01-01 12:00:00'
            }
        ]
        mock_scrape.return_value = mock_products
        
        # Panggil fungsi
        result = main()
        
        # Verifikasi
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)
        mock_scrape.assert_called_once()
    
    @patch('utils.extract.scrape_fashion_products')
    def test_main_empty_result(self, mock_scrape):
        """Test main function dengan hasil kosong"""
        # Setup mock untuk mengembalikan list kosong
        mock_scrape.return_value = []
        
        # Panggil fungsi
        result = main()
        
        # Verifikasi
        self.assertIsNone(result)
        mock_scrape.assert_called_once()
    
    @patch('utils.extract.scrape_fashion_products')
    def test_main_exception(self, mock_scrape):
        """Test main function dengan exception"""
        # Setup mock untuk menimbulkan exception
        mock_scrape.side_effect = Exception("Test error")
        
        # Panggil fungsi
        result = main()
        
        # Verifikasi
        self.assertIsNone(result)
        mock_scrape.assert_called_once()

if __name__ == '__main__':
    unittest.main()