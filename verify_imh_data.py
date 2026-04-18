import json
import os

output_dir = 'output'
json_files = ['daily.json', 'monthly.json', 'quarterly.json', 'on_request.json']

print("=" * 80)
print("统计 JSON 文件中的 imh 数据")
print("=" * 80)

total_programs = 0
total_with_imh = 0
total_with_imh_tables = 0

for json_file in json_files:
    file_path = os.path.join(output_dir, json_file)
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
            programs_count = len(data)
            with_imh_count = 0
            imh_tables_count = 0
            
            print(f"\n{json_file}:")
            print(f"  总程序数量: {programs_count}")
            
            for prog_name, prog_info in data.items():
                if 'input' in prog_info and 'imh' in prog_info['input']:
                    with_imh_count += 1
                    imh_tables_count += len(prog_info['input']['imh'])
            
            print(f"  有 imh 数据的程序: {with_imh_count}")
            print(f"  总 imh 表数量: {imh_tables_count}")
            
            total_programs += programs_count
            total_with_imh += with_imh_count
            total_with_imh_tables += imh_tables_count

print(f"\n{'='*80}")
print(f"总计:")
print(f"  总程序数量: {total_programs}")
print(f"  有 imh 数据的程序: {total_with_imh}")
print(f"  总 imh 表数量: {total_with_imh_tables}")

print(f"\n{'='*80}")
print("示例程序的 imh 数据:")
print("=" * 80)

for json_file in json_files[:2]:
    file_path = os.path.join(output_dir, json_file)
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
            print(f"\n{json_file} 中的示例:")
            count = 0
            for prog_name, prog_info in data.items():
                if 'input' in prog_info and 'imh' in prog_info['input']:
                    print(f"\n  程序: {prog_name}")
                    print(f"  imh 表: {prog_info['input']['imh'][:5]}...")
                    count += 1
                    if count >= 3:
                        break
