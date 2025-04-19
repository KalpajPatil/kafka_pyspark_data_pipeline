import pandas as pd
import os

CSV_DIR = "./output/csv_data"
EXCEL_DIR = "./output/excel_data.xlsx"

def aggregate_csv_to_excel(csv_dir, excel_path):
    all_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]
    
    combined_df = pd.DataFrame()

    # Read the existing Excel file if it exists
    if os.path.exists(excel_path):
        existing_df = pd.read_excel(excel_path, sheet_name='Aggregated Data')
        combined_df = existing_df

    # Aggregate new CSV files
    for file in all_files:
        file_path = os.path.join(csv_dir, file)
        df = pd.read_csv(file_path)
        combined_df = pd.concat([combined_df, df], ignore_index=True)
        #os.remove(file_path)
    
    # Write the combined data back to the Excel file
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        combined_df.to_excel(writer, index=False, sheet_name='Aggregated Data')

    print(f"Data has been aggregated into {excel_path}")

if __name__ == "__main__":
    aggregate_csv_to_excel(CSV_DIR, EXCEL_DIR)
    