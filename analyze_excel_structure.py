import pandas as pd
import os

excel_files = [
    "1_include_relationship.xlsx",
    "2_input_database_tables.xlsx",
    "3_input_library_data.xlsx",
    "4_sas_imported_files_output.xlsx",
    "0.2_sas_path_definition.xlsx"
]

source_dir = 'sourceData'

for excel_file in excel_files:
    file_path = os.path.join(source_dir, excel_file)
    
    if not os.path.exists(file_path):
        print(f"文件不存在: {file_path}")
        continue
    
    print(f"\n{'='*80}")
    print(f"文件名: {excel_file}")
    print(f"{'='*80}")
    
    xl = pd.ExcelFile(file_path)
    
    for sheet_name in xl.sheet_names:
        print(f"\n--- Sheet: {sheet_name} ---")
        
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        
        print(f"行数: {len(df)}")
        print(f"列名: {df.columns.tolist()}")
        
        if len(df) > 0:
            print(f"\n前5行数据:")
            print(df.head(5))
            print(f"\n数据类型:")
            print(df.dtypes)
