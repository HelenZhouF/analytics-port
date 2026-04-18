import json
import os

json_dir = r'c:\workspace\ai\relationship\analytics-port\output'

print("=" * 80)
print("验证所有JSON文件的output.excel更新情况")
print("=" * 80)

json_files = ['daily.json', 'monthly.json', 'quarterly.json', 'on_request.json']

summary = {}

for json_file in json_files:
    json_path = os.path.join(json_dir, json_file)
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    total_programs = len(data)
    total_with_output = 0
    total_with_output_excel = 0
    total_with_output_ds = 0
    
    for program_name, program_info in data.items():
        if 'output' in program_info:
            total_with_output += 1
            output_info = program_info['output']
            if 'excel' in output_info and len(output_info['excel']) > 0:
                total_with_output_excel += 1
            if 'ds' in output_info and len(output_info['ds']) > 0:
                total_with_output_ds += 1
    
    summary[json_file] = {
        'total': total_programs,
        'with_output': total_with_output,
        'with_excel': total_with_output_excel,
        'with_ds': total_with_output_ds
    }
    
    print(f'\n=== {json_file} ===')
    print(f'  总程序数: {total_programs}')
    print(f'  有output属性的程序数: {total_with_output}')
    print(f'  有output.excel的程序数: {total_with_output_excel}')
    print(f'  有output.ds的程序数: {total_with_output_ds}')

print("\n" + "=" * 80)
print("汇总统计")
print("=" * 80)

total_all = sum(s['total'] for s in summary.values())
total_with_excel = sum(s['with_excel'] for s in summary.values())

print(f'\n所有JSON文件总程序数: {total_all}')
print(f'所有JSON文件有output.excel的程序数: {total_with_excel}')

print("\n" + "=" * 80)
print("示例更新后的结构")
print("=" * 80)

# 从daily.json找一个示例
json_path = os.path.join(json_dir, 'daily.json')
with open(json_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

for program_name, program_info in data.items():
    if 'output' in program_info and 'excel' in program_info['output']:
        print(f'\n程序名: {program_name}')
        print(f'  output.excel 结构:')
        for idx, excel_item in enumerate(program_info['output']['excel'][:3]):
            print(f'    [{idx}]')
            if 'path' in excel_item:
                print(f'      path: {excel_item["path"]}')
            if 'from' in excel_item:
                print(f'      from: {excel_item["from"]}')
            if 'name' in excel_item:
                print(f'      name: {excel_item["name"]}')
        break
