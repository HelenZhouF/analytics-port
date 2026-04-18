import pandas as pd
import json
import os

excel_path = r'c:\workspace\ai\relationship\analytics-port\sourceData\5_sas_exported_files_output.xlsx'

print("=== Exported Files sheet ===")
df = pd.read_excel(excel_path, sheet_name='Exported Files')
print(f'总行数: {len(df)}')
print(f'列名: {df.columns.tolist()}')

print("\n前15行数据:")
print(df.head(15))

print(f"\n唯一的 XX_file_name 数量: {df['XX_file_name'].nunique()}")
print(f"唯一的 XX_file_name (非空): {df['XX_file_name'].dropna().nunique()}")

print("\n=== 非空的 XX_file_name 示例 ===")
valid_names = df['XX_file_name'].dropna().unique()
print(f"数量: {len(valid_names)}")
if len(valid_names) > 0:
    print(f"前20个: {valid_names[:20].tolist()}")

print("\n\n=== vbs Files sheet ===")
df_vbs = pd.read_excel(excel_path, sheet_name='vbs Files')
print(f'总行数: {len(df_vbs)}')
print(f'列名: {df_vbs.columns.tolist()}')
print("\n前5行数据:")
print(df_vbs.head(5))

print("\n\n=== JSON文件key示例 ===")
json_dir = r'c:\workspace\ai\relationship\analytics-port\output'
for json_file in ['daily.json', 'monthly.json', 'quarterly.json', 'on_request.json']:
    json_path = os.path.join(json_dir, json_file)
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    keys = list(data.keys())[:10]
    print(f'\n{json_file} ({len(data)} 个程序):')
    print(keys)
