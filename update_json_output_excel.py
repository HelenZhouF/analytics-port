import pandas as pd
import json
import os
import copy

def convert_txt_to_sas(filename):
    if filename is None or pd.isna(filename):
        return None
    filename_str = str(filename).strip()
    return filename_str.replace('.txt', '.sas').replace('.TXT', '.SAS')

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

def load_excel_data(excel_path):
    print(f"读取Excel文件: {excel_path}")
    
    xl = pd.ExcelFile(excel_path)
    all_data = {}
    
    for sheet_name in xl.sheet_names:
        print(f"  处理sheet: {sheet_name}")
        
        if sheet_name == 'Readme':
            continue
        
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
        print(f"    行数: {len(df)}")
        
        for idx, row in df.iterrows():
            xx_file_name = clean_value(row.get('XX_file_name'))
            
            if not xx_file_name:
                continue
            
            sas_name = convert_txt_to_sas(xx_file_name)
            
            if sas_name not in all_data:
                all_data[sas_name] = []
            
            excel_item = {}
            
            output_file_full_path = clean_value(row.get('output_file_full_path'))
            input_dataset_name = clean_value(row.get('input_dataset_name'))
            output_file_name = clean_value(row.get('output_file_name'))
            
            if output_file_full_path:
                excel_item['path'] = output_file_full_path
            if input_dataset_name:
                excel_item['from'] = input_dataset_name
            if output_file_name:
                excel_item['name'] = output_file_name
            
            if excel_item and excel_item not in all_data[sas_name]:
                all_data[sas_name].append(excel_item)
    
    return all_data

def load_json_files(json_dir):
    json_files = ['daily.json', 'monthly.json', 'quarterly.json', 'on_request.json']
    all_json_data = {}
    
    for json_file in json_files:
        file_path = os.path.join(json_dir, json_file)
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                all_json_data[json_file] = data
                print(f"已加载 {json_file}: {len(data)} 个程序")
    
    return all_json_data

def update_json_with_excel(json_data, excel_data, json_file_name):
    updated_count = 0
    matched_programs = 0
    
    for program_name, program_info in json_data.items():
        if program_name in excel_data:
            matched_programs += 1
            excel_items = excel_data[program_name]
            
            if 'output' not in program_info:
                program_info['output'] = {}
            
            if 'excel' not in program_info['output']:
                program_info['output']['excel'] = []
            
            existing_excels = program_info['output']['excel']
            
            for excel_item in excel_items:
                if excel_item not in existing_excels:
                    existing_excels.append(excel_item)
                    updated_count += 1
    
    return updated_count, matched_programs

def save_json_files(json_data_dict, json_dir):
    for json_file, data in json_data_dict.items():
        file_path = os.path.join(json_dir, json_file)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"已保存到: {file_path}")

def main():
    print("=" * 80)
    print("从Excel读取输出文件信息并更新JSON文件")
    print("=" * 80)
    
    source_dir = 'sourceData'
    output_dir = 'output'
    excel_file = '5_sas_exported_files_output.xlsx'
    
    excel_path = os.path.join(source_dir, excel_file)
    
    print("\n1. 从Excel文件加载数据...")
    excel_data = load_excel_data(excel_path)
    print(f"   共加载 {len(excel_data)} 个程序的输出Excel信息")
    
    print("\n2. 加载现有JSON文件...")
    json_data_dict = load_json_files(output_dir)
    
    print("\n3. 更新JSON数据...")
    total_updated = 0
    total_matched = 0
    
    for json_file, json_data in json_data_dict.items():
        updated_count, matched_count = update_json_with_excel(json_data, excel_data, json_file)
        total_updated += updated_count
        total_matched += matched_count
        print(f"   {json_file}: 匹配 {matched_count} 个程序，新增 {updated_count} 个output.excel项")
    
    print(f"\n   总计: 匹配 {total_matched} 个程序，新增 {total_updated} 个output.excel项")
    
    print("\n4. 保存更新后的JSON文件...")
    save_json_files(json_data_dict, output_dir)
    
    print("\n" + "=" * 80)
    print("处理完成!")
    print("=" * 80)
    
    print("\n统计信息:")
    print(f"  - Excel中提取的程序数量: {len(excel_data)}")
    print(f"  - JSON中匹配的程序数量: {total_matched}")
    print(f"  - 新增的output.excel项数量: {total_updated}")
    
    print("\n示例更新后的结构:")
    for program_name, excel_items in excel_data.items():
        print(f"\n  {program_name}:")
        for item in excel_items[:3]:
            print(f"    - {item}")
        if len(excel_items) > 3:
            print(f"    ... 共 {len(excel_items)} 项")
        break

if __name__ == '__main__':
    main()
