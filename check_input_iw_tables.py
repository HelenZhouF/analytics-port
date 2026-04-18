import pandas as pd
import os

file_path = os.path.join('sourceData', '2_input_database_tables.xlsx')

print("=" * 80)
print("检查 2_input_database_tables.xlsx - input_iw_tables sheet")
print("=" * 80)

df = pd.read_excel(file_path, sheet_name='input_iw_tables')

print(f"\n总行数: {len(df)}")
print(f"\n列名: {df.columns.tolist()}")

print(f"\nXX_file_name 列统计:")
print(f"  非空值数量: {df['XX_file_name'].notna().sum()}")
print(f"  唯一值数量: {df['XX_file_name'].nunique()}")

print(f"\nXX_file_full_path 列统计:")
print(f"  非空值数量: {df['XX_file_full_path'].notna().sum()}")
print(f"  唯一值数量: {df['XX_file_full_path'].nunique()}")

print(f"\nInput_data_1 列统计:")
print(f"  非空值数量: {df['Input_data_1'].notna().sum()}")
print(f"\n  Input_data_1 前20个值:")
for i, val in enumerate(df['Input_data_1'].head(20).tolist()):
    print(f"    [{i+1}] {val}")

import re
def extract_filename(path):
    if pd.isna(path) or path is None:
        return None
    path_str = str(path)
    match = re.search(r'[^\\/]+\.(txt|sas|TXT|SAS)$', path_str)
    if match:
        return match.group(0)
    return None

def convert_to_sas_name(filename):
    if filename is None:
        return None
    filename_str = str(filename).strip()
    return filename_str.replace('.txt', '.sas').replace('.TXT', '.SAS')

print(f"\n从 XX_file_full_path 提取文件名并转换为 .sas:")
sas_names = set()
for path in df['XX_file_full_path'].unique():
    filename = extract_filename(path)
    if filename:
        sas_name = convert_to_sas_name(filename)
        sas_names.add(sas_name)
        print(f"  {path}")
        print(f"    -> {filename} -> {sas_name}")

print(f"\n共提取 {len(sas_names)} 个唯一的 SAS 文件名")

print(f"\n按 XX_file_full_path 分组统计:")
path_groups = df.groupby('XX_file_full_path').agg({
    'Input_data_1': ['count', 'first']
}).reset_index()

print(f"  分组数量: {len(path_groups)}")
print(f"\n  前10个分组:")
for i, (_, row) in enumerate(path_groups.head(10).iterrows()):
    path = row['XX_file_full_path']
    count = row[('Input_data_1', 'count')]
    first = row[('Input_data_1', 'first')]
    filename = extract_filename(path)
    sas_name = convert_to_sas_name(filename) if filename else None
    print(f"    [{i+1}] {sas_name}")
    print(f"         Input_data_1 数量: {count}")
    print(f"         第一个值: {first}")
