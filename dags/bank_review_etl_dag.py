from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import json
import logging

# === Default arguments ===
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# === File paths ==='
input_path = '/opt/airflow/dags/Bank_review.csv'
output_path = '/opt/airflow/dags/Bank_cleaned_reviews.json'

# === ETL function ===
def clean_and_engineer_data():
    """Clean and engineer bank review data"""
    
    try:
        # Check if input file exists
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file {input_path} not found")
        
        logging.info(f"Loading data from {input_path}")
        
        # Load data with proper column names based on your sample
        # Your data appears to have these columns (no headers in the file)
        column_names = [
            'reviewer_name', 'review_date', 'address', 'bank_name', 
            'rating', 'review_title', 'review_text', 'bank_logo_url', 
            'satisfaction_level', 'useful_count'
        ]
        
        df = pd.read_csv(input_path, names=column_names, header=None)
        logging.info(f"Loaded {len(df)} raw records")
        
        # === Basic Cleaning ===
        initial_count = len(df)
        
        # Remove rows where essential columns are null
        essential_columns = ['review_date', 'bank_name', 'rating', 'review_text']
        df = df.dropna(subset=essential_columns)
        logging.info(f"Removed {initial_count - len(df)} rows with missing essential data")
        
        # Remove duplicates based on review content and bank
        df = df.drop_duplicates(subset=['review_text', 'bank_name', 'rating'], keep='first')
        logging.info(f"Removed duplicates, {len(df)} records remaining")
        
        # Clean text columns
        def clean_text(text):
            if pd.isna(text):
                return ""
            # Remove extra quotes and normalize whitespace
            text = str(text).strip()
            text = text.replace('"""', '"').replace('""', '"')
            if text.startswith('"') and text.endswith('"'):
                text = text[1:-1]
            return text
        
        df['review_title'] = df['review_title'].apply(clean_text)
        df['review_text'] = df['review_text'].apply(clean_text)
        
        # Normalize bank names
        df['bank_name'] = df['bank_name'].str.upper().str.strip()
        # Handle the 'X' placeholder in your data
        df['bank_name'] = df['bank_name'].replace('X', 'BANK_X')
        
        # === Date Processing ===
        if 'review_date' in df.columns:
            # Handle multiple date formats
            def parse_date(date_str):
                if pd.isna(date_str):
                    return None
                
                date_formats = ['%d/%m/%Y', '%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y']
                
                for fmt in date_formats:
                    try:
                        return pd.to_datetime(str(date_str), format=fmt)
                    except ValueError:
                        continue
                
                # Try pandas automatic parsing
                try:
                    return pd.to_datetime(str(date_str))
                except:
                    return None
            
            df['review_date'] = df['review_date'].apply(parse_date)
            df = df.dropna(subset=['review_date'])
            
            # Extract temporal features
            df['month'] = df['review_date'].dt.month
            df['weekday'] = df['review_date'].dt.day_name()
            df['year'] = df['review_date'].dt.year
            df['quarter'] = df['review_date'].dt.quarter
            
            # Add season
            def get_season(month):
                if month in [12, 1, 2]:
                    return 'Winter'
                elif month in [3, 4, 5]:
                    return 'Spring'
                elif month in [6, 7, 8]:
                    return 'Summer'
                else:
                    return 'Fall'
            
            df['season'] = df['month'].apply(get_season)
            
            logging.info(f"Date processing completed. Date range: {df['review_date'].min()} to {df['review_date'].max()}")
        
        # === Create full review text ===
        def combine_review_text(title, text):
            title = str(title) if pd.notna(title) and title != '' else ''
            text = str(text) if pd.notna(text) else ''
            
            if title and text:
                return f"{title}. {text}"
            elif title:
                return title
            else:
                return text
        
        df['full_review'] = df.apply(
            lambda row: combine_review_text(row['review_title'], row['review_text']), 
            axis=1
        )
        
        # === Add usefulness flag ===
        if 'useful_count' in df.columns:
            df['useful_count'] = pd.to_numeric(df['useful_count'], errors='coerce').fillna(0)
            df['is_useful'] = df['useful_count'] > 0
            logging.info(f"Useful reviews: {df['is_useful'].sum()}/{len(df)}")
        
        # === Regional mapping ===
        address_to_area = {
            'Tunis': 'Grand Tunis',
            'Ariana': 'Grand Tunis',
            'Ben Arous': 'Grand Tunis',
            'Manouba': 'Grand Tunis',
            
            'Bizerte': 'North-East',
            'Nabeul': 'North-East',
            
            'Beja': 'North-West',
            'Jendouba': 'North-West',
            'Kef': 'North-West',
            'Siliana': 'North-West',
            
            'Sousse': 'Central-East',
            'Monastir': 'Central-East',
            'Mahdia': 'Central-East',
            
            'Sfax': 'South-East',
            'Tataouine': 'South-East',
            'Gabes': 'South-East',
            'Medenine': 'South-East',
            
            'Gafsa': 'South-West',
            'Tozeur': 'South-West',
            'Kebili': 'South-West',
            'Kasserine': 'South-West'
        }
        
        if 'address' in df.columns:
            # Clean and standardize addresses
            df['address'] = df['address'].str.title().str.strip()
            df['area_group'] = df['address'].map(address_to_area)
            # Fill unknown areas
            df['area_group'] = df['area_group'].fillna('Other')
            
            logging.info(f"Regional distribution: {df['area_group'].value_counts().to_dict()}")
        
        # === Additional features for sentiment analysis ===
        df['text_length'] = df['full_review'].str.len()
        df['word_count'] = df['full_review'].str.split().str.len()
        df['exclamation_count'] = df['full_review'].str.count('!')
        df['question_count'] = df['full_review'].str.count('\\?')

        
        # Extract account type
        def extract_account_type(text):
            if pd.isna(text):
                return 'Unknown'
            
            text_lower = str(text).lower()
            if 'salary account' in text_lower:
                return 'Salary'
            elif 'savings account' in text_lower or 'saving account' in text_lower:
                return 'Savings'
            elif 'current account' in text_lower:
                return 'Current'
            else:
                return 'Unknown'
        
        df['account_type'] = df['full_review'].apply(extract_account_type)
        
        # === Data validation ===
        # Ensure ratings are in valid range
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
        df = df[(df['rating'] >= 1) & (df['rating'] <= 5)]
        
        # Remove very short reviews (likely spam or incomplete)
        df = df[df['text_length'] >= 10]
        
        logging.info(f"Final dataset: {len(df)} records")
        logging.info(f"Columns: {list(df.columns)}")
        logging.info(f"Rating distribution: {df['rating'].value_counts().sort_index().to_dict()}")
        
        # === Create output directory if it doesn't exist ===
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)
        
        # === Save as JSON ===
        # Convert datetime to string for JSON serialization
        df_for_json = df.copy()
        if 'review_date' in df_for_json.columns:
            df_for_json['review_date'] = df_for_json['review_date'].dt.strftime('%Y-%m-%d')
        
        records = df_for_json.to_dict(orient='records')
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(records, f, ensure_ascii=False, indent=2, default=str)
        
        logging.info(f"Successfully saved cleaned data with {len(records)} records to {output_path}")
        
        # Return summary statistics
        return {
            'total_records': len(records),
            'unique_banks': df['bank_name'].nunique(),
            'date_range': f"{df['review_date'].min()} to {df['review_date'].max()}",
            'regional_distribution': df['area_group'].value_counts().to_dict(),
            'account_types': df['account_type'].value_counts().to_dict()
        }
        
    except Exception as e:
        logging.error(f"Error in ETL process: {str(e)}")
        raise

def validate_output():
    """Validate the output file"""
    try:
        if not os.path.exists(output_path):
            raise FileNotFoundError(f"Output file {output_path} not found")
        
        with open(output_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not data:
            raise ValueError("Output file is empty")
        
        logging.info(f"Validation passed: {len(data)} records in output file")
        
        # Basic validation checks
        required_fields = ['bank_name', 'rating', 'full_review', 'area_group']
        sample_record = data[0]
        
        for field in required_fields:
            if field not in sample_record:
                raise ValueError(f"Required field '{field}' missing from output")
        
        logging.info("All validation checks passed!")
        return True
        
    except Exception as e:
        logging.error(f"Validation failed: {str(e)}")
        raise

# === DAG Definition ===
with DAG(
    dag_id='bank_review_etl_v2',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger
    catchup=False,
    description='ETL pipeline for bank reviews with proper error handling',
    tags=['etl', 'banking', 'data_processing']
) as dag:
    
    # Task 1: Clean and engineer data
    clean_data_task = PythonOperator(
        task_id='clean_and_engineer_data',
        python_callable=clean_and_engineer_data
    )
    
    # Task 2: Validate output
    validate_task = PythonOperator(
        task_id='validate_output',
        python_callable=validate_output
    )
    
    # Set task dependencies
    clean_data_task >> validate_task
