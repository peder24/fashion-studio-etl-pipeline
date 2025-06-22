# Fashion Studio ETL Pipeline

## Setup Instructions

### 1. Clone Repository
\\\ash
git clone https://github.com/peder24/fashion-studio-etl-pipeline.git
cd fashion-studio-etl-pipeline
\\\

### 2. Install Dependencies
\\\ash
pip install -r requirements.txt
\\\

### 3. Setup Google Sheets API Credentials
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing project
3. Enable Google Sheets API
4. Create Service Account credentials
5. Download the JSON credential file
6. Rename it to \google-sheets-api.json\ and place in project root
7. Use \google-sheets-api-template.json\ as reference for the structure

### 4. Run the Pipeline
\\\ash
python main.py
\\\

### 5. Run Tests
\\\ash
pytest tests/
\\\

## Project Structure
- \main.py\ - Main ETL pipeline
- \utils/\ - ETL utility functions
  - \extract.py\ - Data extraction functions
  - \	ransform.py\ - Data transformation functions  
  - \load.py\ - Data loading functions
- \	ests/\ - Unit tests
- \products.csv\ - Sample data file

## Important Notes
- Never commit API credentials to version control
- The \google-sheets-api.json\ file is ignored by git for security
- Use environment variables for production deployments
