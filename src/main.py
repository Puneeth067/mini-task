import os
import pandas as pd
from bs4 import BeautifulSoup
import pyarrow

def find_and_convert_file(ingestion_dir):

    files = os.listdir(ingestion_dir)
    csv_files = [f for f in files if f.endswith(".csv")]
    xlsx_files = [f for f in files if f.endswith(".xlsx")]
    json_files = [f for f in files if f.endswith(".json")]

    if csv_files:
        print(f"CSV file found at injestion folder")
        return os.path.join(ingestion_dir, csv_files[0])

    if xlsx_files:
        xlsx_path = os.path.join(ingestion_dir, xlsx_files[0])
        csv_path = xlsx_path.replace(".xlsx", ".csv")
        df = pd.read_excel(xlsx_path)
        df.to_csv(csv_path, index=False)
        print(f"Converted {xlsx_files[0]} to CSV")
        return csv_path

    if json_files:
        json_path = os.path.join(ingestion_dir, json_files[0])
        csv_path = json_path.replace(".json", ".csv")
        df = pd.read_json(json_path)
        df.to_csv(csv_path, index=False)
        print(f"Converted {json_files[0]} to CSV")
        return csv_path

    raise FileNotFoundError("No valid CSV, Excel, or JSON file found in the ingestion folder.")

def extract_employee_data(file_path):
    
    df = pd.read_csv(file_path, encoding="ISO-8859-1")

    # Extract relevant data using BeautifulSoup

    return df


def save_to_parquet(df, output_path):
    df.to_parquet(output_path, engine='pyarrow', index=False)
    print(f" Parquet file generated: {output_path}")

def lambdaHandler(event, context):

    scraper_info = event.get("scraper_input", {})
    scraper_name = scraper_info.get("scraper_name", "unknown_scraper")
    run_id = scraper_info.get("run_scraper_id", "000")

    print(f"Running Scraper: {scraper_name} | Run ID: {run_id}")

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    ingestion_dir = os.path.join(base_dir, "ingestion")
    
    csv_path = find_and_convert_file(ingestion_dir)

    output_path = os.path.join(ingestion_dir, "employee_data.parquet")

    employee_df = extract_employee_data(csv_path)

    save_to_parquet(employee_df, output_path)

    return {
        "statusCode": 200,
        "body": f"Parquet file generated at {output_path}"
    }

if __name__ == "__main__":
    test_input = {
        "scraper_input": {
            "scraper_name": "csv_100",
            "run_scraper_id": "100"
        }
    }
    lambdaHandler(test_input, "")
