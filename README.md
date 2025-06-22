# Fashion Studio ETL Pipeline

## Setup Instructions

### 1. Clone Repository
git clone https://github.com/peder24/fashion-studio-etl-pipeline.git
cd fashion-studio-etl-pipeline

### 2. Install Dependencies
pip install -r requirements.txt

### 3. Setup Google Sheets API Credentials
- Go to Google Cloud Console
- Create a new project or select existing project
- Enable Google Sheets API
- Create Service Account credentials
- Download the JSON credential file
- Rename it to google-sheets-api.json and place in project root
- Use google-sheets-api-template.json as reference for the structure

### 4. Run the Pipeline
python main.py

### 5. Run Tests
pytest tests/

## Project Structure
- main.py - Main ETL pipeline
- utils/ - ETL utility functions
  - extract.py - Data extraction functions
  - transform.py - Data transformation functions  
- load.py - Data loading functions
- tests/ - Unit tests
- products.csv - Sample data file
