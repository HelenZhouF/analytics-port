import json
import os

json_dir = r'c:\workspace\ai\relationship\analytics-port\output'

print("=" * 80)
print("检查JSON文件中output.excel的状态")
print("=" * 80)

json_file = 'daily.json'
json_path = os.path.join(json_dir, json_file)
with open(json_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

check_programs = ['OGL_Remap_Daily.sas', 'Daily_IW_Check2.sas', 'Daily_TP_Check.sas']

for program in check_programs:
    if program in data:
        program_info = data[program]
        print(f'\n=== {program} ===')
        print(f'  keys: {list(program_info.keys())}')
        if 'output' in program_info:
            output_info = program_info['output']
            print(f'  output keys: {list(output_info.keys())}')
            if 'excel' in output_info:
                print(f'  output.excel: {output_info["excel"]}')
            if 'ds' in output_info:
                print(f'  output.ds: {output_info["ds"][:2] if len(output_info["ds"]) > 2 else output_info["ds"]}')
        else:
            print('  没有output属性')

print("\n" + "=" * 80)
print("统计所有程序中output.excel的情况")
print("=" * 80)

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

print(f'\n总程序数: {len(data)}')
print(f'有output属性的程序数: {total_with_output}')
print(f'有output.excel的程序数: {total_with_output_excel}')
print(f'有output.ds的程序数: {total_with_output_ds}')

if total_with_output_excel > 0:
    print("\n有output.excel的程序示例:")
    count = 0
    for program_name, program_info in data.items():
        if 'output' in program_info and 'excel' in program_info['output'] and len(program_info['output']['excel']) > 0:
            print(f"  - {program_name}: {program_info['output']['excel']}")
            count += 1
            if count >= 5:
                break
