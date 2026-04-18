import pandas as pd
import os
import re

excel_files = {
    "include": "1_include_relationship.xlsx",
    "imh": "2_input_database_tables.xlsx",
    "ds": "3_input_library_data.xlsx",
    "excel": "4_sas_imported_files_output.xlsx",
    "libname": "0.2_sas_path_definition.xlsx"
}

sheets = {
    "include": "Include_Relationship",
    "imh": "imh_input_tables",
    "ds": "libraries_source_data",
    "excel": "Imported Files",
    "libname": "libname_definition"
}

source_dir = 'sourceData'

def extract_filename(path):
    if pd.isna(path):
        return None
    path_str = str(path)
    match = re.search(r'[^\\/]+\.(txt|sas|TXT|SAS)$', path_str)
    if match:
        return match.group(0)
    return None

for data_type, excel_file in excel_files.items():
    file_path = os.path.join(source_dir, excel_file)
    sheet_name = sheets[data_type]
    
    if not os.path.exists(file_path):
        print(f"文件不存在: {file_path}")
        continue
    
    print(f"\n{'='*80}")
    print(f"数据类型: {data_type}")
    print(f"文件: {excel_file}")
    print(f"Sheet: {sheet_name}")
    print(f"{'='*80}")
    
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    
    print(f"\nXX_file_full_path 列的前10个值:")
    for i, val in enumerate(df['XX_file_full_path'].head(10).tolist()):
        print(f"  [{i+1}] {val}")
        extracted = extract_filename(val)
        if extracted:
            print(f"       提取的文件名: {extracted}")
            sas_name = extracted.replace('.txt', '.sas').replace('.TXT', '.SAS')
            print(f"       转换为SAS: {sas_name}")
    
    print(f"\n统计:")
    total = len(df)
    xx_file_name_not_null = df['XX_file_name'].notna().sum()
    xx_file_full_path_not_null = df['XX_file_full_path'].notna().sum()
    
    print(f"  总行数: {total}")
    print(f"  XX_file_name 非空: {xx_file_name_not_null}")
    print(f"  XX_file_full_path 非空: {xx_file_full_path_not_null}")
    
    extracted_from_full_path = df['XX_file_full_path'].apply(extract_filename).notna().sum()
    print(f"  从 XX_file_full_path 提取到文件名: {extracted_from_full_path}")
    
    print(f"\n尝试从 XX_file_full_path 提取的唯一文件名 (前20个):")
    extracted_filenames = df['XX_file_full_path'].apply(extract_filename).dropna().unique()
    for i, fn in enumerate(extracted_filenames[:20]):
        sas_name = fn.replace('.txt', '.sas').replace('.TXT', '.SAS')
        print(f"  [{i+1}] {fn} -> {sas_name}")
