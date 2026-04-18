import pandas as pd
import json
import os
import re
import sys

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.stderr.reconfigure(encoding='utf-8', errors='replace')

def clean_value(value):
    if pd.isna(value) or value is None:
        return ""
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return value
    if isinstance(value, str):
        return value.strip()
    return value

def convert_to_sas_name(filename):
    if filename is None:
        return None
    filename_str = str(filename).strip()
    return filename_str.replace('.txt', '.sas').replace('.TXT', '.SAS')

def extract_output_ds_info(output_data, input_data):
    if not isinstance(output_data, str) or '.' not in output_data:
        return None
    
    output_data = output_data.strip()
    
    dot_index = output_data.find('.')
    if dot_index <= 0 or dot_index >= len(output_data) - 1:
        return None
    
    lib = output_data[:dot_index]
    name = output_data[dot_index + 1:]
    
    ds_info = {
        'libname': output_data,
        'name': name,
        'lib': lib
    }
    
    if input_data and isinstance(input_data, str) and input_data.strip():
        ds_info['from'] = input_data.strip()
    
    return ds_info

def load_output_data_from_excel():
    excel_file = 'sourceData/2_data_flow_information.xlsx'
    
    if not os.path.exists(excel_file):
        print(f"错误: Excel文件不存在: {excel_file}")
        return {}
    
    print("=" * 80)
    print("从Excel文件加载输出数据流信息")
    print("=" * 80)
    
    xls = pd.ExcelFile(excel_file)
    sheet_names = xls.sheet_names
    print(f"\nExcel中的Sheet: {sheet_names}")
    
    all_output_data = {}
    total_records = 0
    matched_records = 0
    
    data_sheets = []
    for sheet in sheet_names:
        if 'Readme' not in sheet and 'readme' not in sheet:
            data_sheets.append(sheet)
    
    print(f"需要处理的数据Sheet: {data_sheets}")
    
    for sheet in data_sheets:
        print(f"\n处理Sheet: {sheet} ...")
        
        df = pd.read_excel(excel_file, sheet_name=sheet)
        print(f"  总行数: {len(df)}")
        
        if 'XX_file_name' not in df.columns:
            print(f"  警告: 未找到 'XX_file_name' 列，跳过此Sheet")
            continue
        
        if 'Output_data_1' not in df.columns:
            print(f"  警告: 未找到 'Output_data_1' 列，跳过此Sheet")
            continue
        
        for idx, row in df.iterrows():
            total_records += 1
            
            xx_file_name = clean_value(row.get('XX_file_name'))
            output_data_1 = clean_value(row.get('Output_data_1'))
            input_data_1 = clean_value(row.get('Input_data_1'))
            
            if not xx_file_name:
                continue
            
            if not output_data_1 or not isinstance(output_data_1, str):
                continue
            
            if '.' not in output_data_1:
                continue
            
            sas_file_name = convert_to_sas_name(xx_file_name)
            if not sas_file_name:
                continue
            
            ds_info = extract_output_ds_info(output_data_1, input_data_1)
            if ds_info is None:
                continue
            
            matched_records += 1
            
            if sas_file_name not in all_output_data:
                all_output_data[sas_file_name] = []
            
            if ds_info not in all_output_data[sas_file_name]:
                all_output_data[sas_file_name].append(ds_info)
    
    print(f"\n{'='*80}")
    print("Excel数据加载统计")
    print(f"{'='*80}")
    print(f"  总处理记录数: {total_records}")
    print(f"  匹配条件的记录数: {matched_records}")
    print(f"  涉及的SAS程序数: {len(all_output_data)}")
    
    if all_output_data:
        print(f"\n示例数据:")
        for sas_name, ds_list in list(all_output_data.items())[:3]:
            print(f"\n  {sas_name}:")
            for ds in ds_list[:3]:
                print(f"    - {ds}")
    
    return all_output_data

def load_json_files():
    output_dir = 'output'
    json_files = ['daily.json', 'monthly.json', 'quarterly.json', 'on_request.json']
    
    all_json_data = {}
    
    print(f"\n{'='*80}")
    print("加载JSON文件")
    print(f"{'='*80}")
    
    for json_file in json_files:
        file_path = os.path.join(output_dir, json_file)
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                all_json_data[json_file] = data
                print(f"  已加载 {json_file}: {len(data)} 个程序")
    
    return all_json_data

def update_json_with_output(json_data_dict, output_data):
    print(f"\n{'='*80}")
    print("更新JSON文件")
    print(f"{'='*80}")
    
    total_matched = 0
    total_updated = 0
    total_ds_added = 0
    
    for json_file, json_data in json_data_dict.items():
        print(f"\n处理 {json_file}:")
        file_matched = 0
        file_updated = 0
        file_ds_added = 0
        
        for program_name, ds_list in output_data.items():
            if program_name in json_data:
                file_matched += 1
                program_info = json_data[program_name]
                
                if 'output' not in program_info:
                    program_info['output'] = {}
                
                if 'ds' not in program_info['output']:
                    program_info['output']['ds'] = []
                
                existing_ds = program_info['output']['ds']
                
                for ds_info in ds_list:
                    if ds_info not in existing_ds:
                        existing_ds.append(ds_info)
                        file_ds_added += 1
                        if file_updated == 0:
                            file_updated = 1
        
        total_matched += file_matched
        total_updated += file_updated
        total_ds_added += file_ds_added
        
        print(f"  匹配程序数: {file_matched}")
        print(f"  更新程序数: {file_updated}")
        print(f"  新增ds记录数: {file_ds_added}")
    
    print(f"\n{'='*80}")
    print("总计")
    print(f"{'='*80}")
    print(f"  匹配程序总数: {total_matched}")
    print(f"  更新程序总数: {total_updated}")
    print(f"  新增ds记录总数: {total_ds_added}")
    
    return total_matched, total_updated, total_ds_added

def save_json_files(json_data_dict):
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"\n{'='*80}")
    print("保存更新后的JSON文件")
    print(f"{'='*80}")
    
    for json_file, data in json_data_dict.items():
        file_path = os.path.join(output_dir, json_file)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"  已保存: {file_path}")

def show_sample_updates(json_data_dict, output_data):
    print(f"\n{'='*80}")
    print("示例更新结果")
    print(f"{'='*80}")
    
    for program_name, ds_list in list(output_data.items())[:5]:
        found = False
        for json_file, json_data in json_data_dict.items():
            if program_name in json_data:
                found = True
                program_info = json_data[program_name]
                print(f"\n程序: {program_name} ({json_file})")
                if 'output' in program_info and 'ds' in program_info['output']:
                    print(f"  output.ds:")
                    for ds in program_info['output']['ds']:
                        print(f"    - {ds}")
                break
        if not found:
            print(f"\n程序: {program_name} (未在JSON中找到)")

def main():
    print("\n" + "=" * 80)
    print("从Excel读取输出数据流信息并更新JSON文件")
    print("=" * 80)
    
    output_data = load_output_data_from_excel()
    
    if not output_data:
        print("\n警告: 未找到符合条件的数据，程序退出。")
        return
    
    json_data_dict = load_json_files()
    
    if not json_data_dict:
        print("\n警告: 未找到JSON文件，程序退出。")
        return
    
    update_json_with_output(json_data_dict, output_data)
    
    save_json_files(json_data_dict)
    
    show_sample_updates(json_data_dict, output_data)
    
    print(f"\n{'='*80}")
    print("处理完成!")
    print(f"{'='*80}")

if __name__ == '__main__':
    main()
