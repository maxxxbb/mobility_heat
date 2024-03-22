import pandas as pd
import os

# Saves aggregated trip records (at the pickup level) in parquet files to CSV files for other functions



parquet_files = {
    'PU_FHV': 'Data/Pooled_data/PU/final/final_data_FHV_PU.parquet',
    'PU_YG': 'Data/Pooled_data/PU/final/final_data_YG_PU.parquet',
    'Chi_data': 'Data/Chicago_data/chicago_TNP2019_regression.parquet'
}

for key, parquet_path in parquet_files.items():
    df = pd.read_parquet(parquet_path)
    
    csv_path = parquet_path.replace('.parquet', '.csv')
    
    df.to_csv(csv_path, index=False)
    
    print(f'{key} data saved to CSV at: {csv_path}')