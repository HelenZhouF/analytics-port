import pandas as pd
import json
import os
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

def get_sas_name_from_row(row):
    xx_file_name = row.get('XX_file_name')
    xx_file_full_path = row.get('XX_file_full_path')
    
    if xx_file_name is not None and not pd.isna(xx_file_name) and str(xx_file_name).strip() != '':
        filename = str(xx_file_name).strip()
    elif xx_file_full_path is not None and not pd.isna(xx_file_full_path):
        filename = extract_filename(xx_file_full_path)
        if filename is None:
            return None
    else:
        return None
    
    return convert_to_sas_name(filename)

print("=" * 80)
print("检查 imh 数据提取问题")
print("=" * 80)

file_path = os.path.join('sourceData', '2_input_database_tables.xlsx')
df_imh = pd.read_excel(file_path, sheet_name='imh_input_tables')

print(f"\n1. imh_input_tables sheet 统计:")
print(f"   总行数: {len(df_imh)}")
print(f"   唯一的 XX_file_full_path 数量: {df_imh['XX_file_full_path'].nunique()}")

imh_programs = {}
for idx, row in df_imh.iterrows():
    sas_name = get_sas_name_from_row(row)
    if sas_name:
        if sas_name not in imh_programs:
            imh_programs[sas_name] = []
        
        input_data = str(row.get('Input_data_1', '')).strip().rstrip(';')
        if input_data and input_data not in imh_programs[sas_name]:
            imh_programs[sas_name].append(input_data)

print(f"\n2. 从 imh_input_tables 提取的程序:")
print(f"   程序数量: {len(imh_programs)}")
for prog, imh_list in list(imh_programs.items())[:10]:
    print(f"   - {prog}: {len(imh_list)} 个 imh 表")

output_dir = 'output'
json_files = ['daily.json', 'monthly.json', 'quarterly.json', 'on_request.json']

all_json_programs = {}
for json_file in json_files:
    file_path = os.path.join(output_dir, json_file)
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            for prog_name, prog_data in data.items():
                all_json_programs[prog_name] = {
                    'file': json_file,
                    'has_imh': 'input' in prog_data and 'imh' in prog_data['input']
                }

print(f"\n3. JSON 文件中的程序统计:")
print(f"   总程序数量: {len(all_json_programs)}")

matched_programs = []
unmatched_programs = []

for prog in imh_programs:
    if prog in all_json_programs:
        matched_programs.append(prog)
    else:
        unmatched_programs.append(prog)

print(f"\n4. imh 程序匹配情况:")
print(f"   匹配成功: {len(matched_programs)} 个")
print(f"   匹配失败: {len(unmatched_programs)} 个")

if unmatched_programs:
    print(f"\n   匹配失败的程序:")
    for prog in unmatched_programs[:20]:
        print(f"   - {prog}")

print(f"\n5. 匹配成功的程序详情 (前10个):")
for prog in matched_programs[:10]:
    json_info = all_json_programs[prog]
    print(f"\n   程序: {prog}")
    print(f"   来自 JSON 文件: {json_info['file']}")
    print(f"   JSON 中已有 imh: {json_info['has_imh']}")
    print(f"   imh 表数量: {len(imh_programs[prog])}")
    print(f"   imh 表: {imh_programs[prog][:3]}...")

print(f"\n6. 检查 JSON 文件中实际有多少程序有 imh 字段:")
total_with_imh = 0
for prog, info in all_json_programs.items():
    if info['has_imh']:
        total_with_imh += 1

print(f"   JSON 中已有 imh 字段的程序数量: {total_with_imh}")
