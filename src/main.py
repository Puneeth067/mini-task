import os
import pandas as pd
from bs4 import BeautifulSoup

def find_and_convert_file(ingestion_dir):
    """Finds a file in the ingestion folder, converts it to CSV if needed, and returns the CSV path."""

    # Get all files in the ingestion folder
    files = os.listdir(ingestion_dir)
    csv_files = [f for f in files if f.endswith(".csv")]
    xlsx_files = [f for f in files if f.endswith(".xlsx")]
    json_files = [f for f in files if f.endswith(".json")]

    # If CSV exists, use it directly
    if csv_files:
        print(f"CSV file found at injestion folder")
        return os.path.join(ingestion_dir, csv_files[0])

    # If Excel file exists, convert it to CSV
    if xlsx_files:
        xlsx_path = os.path.join(ingestion_dir, xlsx_files[0])
        csv_path = xlsx_path.replace(".xlsx", ".csv")
        df = pd.read_excel(xlsx_path)
        df.to_csv(csv_path, index=False)
        print(f"Converted {xlsx_files[0]} to CSV")
        return csv_path

    # If JSON file exists, convert it to CSV
    if json_files:
        json_path = os.path.join(ingestion_dir, json_files[0])
        csv_path = json_path.replace(".json", ".csv")
        df = pd.read_json(json_path)
        df.to_csv(csv_path, index=False)
        print(f"Converted {json_files[0]} to CSV")
        return csv_path

    # If no valid file is found, raise an error
    raise FileNotFoundError("No valid CSV, Excel, or JSON file found in the ingestion folder.")

def extract_employee_data(file_path):
    """Reads CSV, extracts relevant data using BeautifulSoup, and processes it."""
    
    # Load CSV file into DataFrame
    df = pd.read_csv(file_path, encoding="ISO-8859-1")

    # Check if an 'HTML' column exists (modify based on actual data structure)
    if 'html_content' in df.columns:
        df['parsed_data'] = df['html_content'].apply(lambda x: BeautifulSoup(x, "lxml").text if pd.notna(x) else "")

    return df

def save_to_parquet(df, output_path):
    """Saves the processed data to a Parquet file."""
    df.to_parquet(output_path, engine='fastparquet', index=False)
    print(f" Parquet file generated: {output_path}")

def lambdaHandler(event, context):
    """AWS Lambda Handler Function"""

    # Extract scraper details
    scraper_info = event.get("scraper_input", {})
    scraper_name = scraper_info.get("scraper_name", "unknown_scraper")
    run_id = scraper_info.get("run_scraper_id", "000")

    print(f"Running Scraper: {scraper_name} | Run ID: {run_id}")

    # Define paths
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    ingestion_dir = os.path.join(base_dir, "ingestion")
    
    # Find and convert a file to CSV if necessary
    csv_path = find_and_convert_file(ingestion_dir)

    # Define Parquet output path
    output_path = os.path.join(ingestion_dir, "employee_data.parquet")

    # Extract and process employee data
    employee_df = extract_employee_data(csv_path)

    # Save as Parquet
    save_to_parquet(employee_df, output_path)

    return {
        "statusCode": 200,
        "body": f"Parquet file generated at {output_path}"
    }

# Run locally for testing
if __name__ == "__main__":
    test_input = {
        "scraper_input": {
            "scraper_name": "csv_100",
            "run_scraper_id": "100"
        }
    }
    lambdaHandler(test_input, "")
