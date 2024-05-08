import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
import os
import sys

def normalize_dates(file_path, output_dir):
    match = re.search(r'postsInformation_(.+?)_(\d{4}-\d{2}-\d{2})\.csv$', file_path)
    if match:
        place = match.group(1)
        reference_date = match.group(2)
    else:
        raise ValueError("Filename does not contain a valid place and reference date.")
    
    ref_date = datetime.strptime(reference_date, '%Y-%m-%d').date()  # Ensuring ref_date is a date object
    df = pd.read_csv(file_path)
    normalized_data = []
    
    for i in range(len(df)):
        row = df.iloc[i]
        original_date = row['time']

        if not isinstance(original_date, str):
            continue
        
        if '在線上' in original_date or original_date.startswith('+'):
            continue
        
        try:
            if '天' in original_date:
                days_ago = int(original_date.replace('天', ''))
                normalized_date = (ref_date - timedelta(days=days_ago))
            elif '小时' in original_date or '小時' in original_date or '分鐘' in original_date:
                normalized_date = ref_date
            elif '年' in original_date:
                normalized_date = datetime.strptime(original_date, '%Y年%m月%d日').date()
            elif '月' in original_date and '日' in original_date:
                date_part = original_date.split('上午')[0].split('下午')[0]
                normalized_date = datetime.strptime(f'{ref_date.year}年{date_part}', '%Y年%m月%d日').date()
            elif '月' in original_date:
                normalized_date = datetime.strptime(f'{ref_date.year}年{original_date}', '%Y年%m月%d日').date()
            else:
                normalized_date = ref_date.date()
        except Exception as e:
            print(f"Error processing date '{original_date}' at index {i}: {e}")
            normalized_date = None
        
        normalized_data.append([row['id'], row['attraction_id'], row['poster_name'], row['content'], row['post_link'], 'fb', normalized_date])
    
    normalized_df = pd.DataFrame(normalized_data, columns=['id', 'attraction_id', 'author', 'description', 'url', 'source_platform_id', 'created_at'])
    output_filename = f"Normalization_{place}_{reference_date}.csv"
    output_path = os.path.join(output_dir, output_filename)
    normalized_df.to_csv(output_path, index=False, encoding='utf_8_sig')  # Use UTF-8 with BOM for better compatibility
    return output_path

def process_all_files(source_dir):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, 'normalized')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    source_dir_path = os.path.join(script_dir, 'source')
    for filename in os.listdir(source_dir_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(source_dir_path, filename)
            try:
                output_path = normalize_dates(file_path, output_dir)
                print(f"Processed {filename}: Output saved to {output_path}")
            except Exception as e:
                print(f"Failed to process {filename}: {str(e)}")

def main():
    process_all_files(os.path.dirname(os.path.abspath(__file__)))

if __name__ == "__main__":
    main()