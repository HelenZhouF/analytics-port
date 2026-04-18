import pandas as pd
import json
import os

def convert_txt_to_sas(filename):
    if filename is None or pd.isna(filename):
        return None
    filename_str = str(filename).strip()
    return filename_str.replace('.txt', '.sas').replace('.TXT', '.SAS')

excel_path = r'c:\workspace\ai\relationship\analytics-port\sourceData\5_sas_exported_files_output.xlsx'
json_dir = r'c:\workspace\ai\relationship\analytics-port\output'

print("=" * 80)
print("分析匹配情况")
print("=" * 80)

df = pd.read_excel(excel_path, sheet_name='Exported Files')
print(f"\nExcel 'Exported Files' sheet 总行数: {len(df)}")

df_valid = df.dropna(subset=['XX_file_name'])
print(f"有有效 XX_file_name 的行数: {len(df_valid)}")

df_valid['sas_name'] = df_valid['XX_file_name'].apply(convert_txt_to_sas)
unique_sas_names = df_valid['sas_name'].unique()
print(f"\nExcel中唯一的SAS文件名数量: {len(unique_sas_names)}")

json_files = ['daily.json', 'monthly.json', 'quarterly.json', 'on_request.json']
all_json_keys = set()

for json_file in json_files:
    json_path = os.path.join(json_dir, json_file)
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    all_json_keys.update(data.keys())
    print(f"\n{json_file}: {len(data)} 个程序")

print(f"\n所有JSON文件中唯一的程序名数量: {len(all_json_keys)}")

matched_names = []
unmatched_names = []

for sas_name in unique_sas_names:
    if sas_name in all_json_keys:
        matched_names.append(sas_name)
    else:
        unmatched_names.append(sas_name)

print(f"\n匹配的SAS文件名数量: {len(matched_names)}")
print(f"不匹配的SAS文件名数量: {len(unmatched_names)}")

if matched_names:
    print(f"\n匹配的SAS文件名示例 (前20个):")
    for name in matched_names[:20]:
        print(f"  - {name}")

if unmatched_names:
    print(f"\n不匹配的SAS文件名示例 (前20个):")
    for name in unmatched_names[:20]:
        print(f"  - {name}")

print("\n" + "=" * 80)
print("查看Excel中数据列的内容示例")
print("=" * 80)

df_sample = df_valid[['XX_file_name', 'sas_name', 'input_dataset_name', 'output_file_full_path', 'output_file_name']].head(10)
print("\nExcel数据示例:")
print(df_sample.to_string())
