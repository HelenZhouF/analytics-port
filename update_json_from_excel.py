import pandas as pd
import json
import os
import re

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

def extract_filename_from_path(path):
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

def get_sas_name(row):
    xx_file_name = row.get('XX_file_name')
    xx_file_full_path = row.get('XX_file_full_path')
    
    if xx_file_name is not None and not pd.isna(xx_file_name) and str(xx_file_name).strip() != '':
        filename = str(xx_file_name).strip()
    elif xx_file_full_path is not None and not pd.isna(xx_file_full_path):
        filename = extract_filename_from_path(xx_file_full_path)
        if filename is None:
            return None
    else:
        return None
    
    return convert_to_sas_name(filename)

def load_data_from_excel():
    source_dir = 'sourceData'
    
    excel_configs = [
        {
            'file': '1_include_relationship.xlsx',
            'sheet': 'Include_Relationship',
            'data_type': 'include',
            'columns': ['Included_File_Name']
        },
        {
            'file': '2_input_database_tables.xlsx',
            'sheet': 'imh_input_tables',
            'data_type': 'imh',
            'columns': ['Input_data_1']
        },
        {
            'file': '2_input_database_tables.xlsx',
            'sheet': 'input_iw_tables',
            'data_type': 'imh',
            'columns': ['Input_data_1']
        },
        {
            'file': '3_input_library_data.xlsx',
            'sheet': 'libraries_source_data',
            'data_type': 'ds',
            'columns': ['Library_Name', 'Dataset_Name']
        },
        {
            'file': '4_sas_imported_files_output.xlsx',
            'sheet': 'Imported Files',
            'data_type': 'excel',
            'columns': ['output_dataset_name', 'input_file_name', 'input_file_path']
        },
        {
            'file': '0.2_sas_path_definition.xlsx',
            'sheet': 'libname_definition',
            'data_type': 'libname',
            'columns': ['libname', 'definition']
        }
    ]
    
    all_data = {}
    
    for config in excel_configs:
        file_path = os.path.join(source_dir, config['file'])
        
        if not os.path.exists(file_path):
            print(f"警告: 文件不存在 {file_path}")
            continue
        
        print(f"读取 {config['file']} - {config['sheet']} ...")
        
        df = pd.read_excel(file_path, sheet_name=config['sheet'])
        
        for idx, row in df.iterrows():
            sas_name = get_sas_name(row)
            
            if sas_name is None:
                continue
            
            if sas_name not in all_data:
                all_data[sas_name] = {
                    'imh': [],
                    'ds': [],
                    'excel': [],
                    'libname': [],
                    'include': []
                }
            
            data_type = config['data_type']
            
            if data_type == 'include':
                included_file = clean_value(row.get('Included_File_Name'))
                if included_file:
                    file_name = os.path.basename(included_file)
                    include_item = {
                        'path': included_file,
                        'name': file_name
                    }
                    if include_item not in all_data[sas_name]['include']:
                        all_data[sas_name]['include'].append(include_item)
            
            elif data_type == 'imh':
                input_data = clean_value(row.get('Input_data_1'))
                if input_data:
                    input_data = input_data.rstrip(';')
                    if input_data not in all_data[sas_name]['imh']:
                        all_data[sas_name]['imh'].append(input_data)
            
            elif data_type == 'ds':
                lib_name = clean_value(row.get('Library_Name'))
                ds_name = clean_value(row.get('Dataset_Name'))
                if lib_name and ds_name:
                    ds_item = {
                        'name': ds_name,
                        'lib': lib_name
                    }
                    if ds_item not in all_data[sas_name]['ds']:
                        all_data[sas_name]['ds'].append(ds_item)
            
            elif data_type == 'excel':
                output_ds = clean_value(row.get('output_dataset_name'))
                input_file = clean_value(row.get('input_file_name'))
                input_path = clean_value(row.get('input_file_path'))
                
                if output_ds or input_file:
                    excel_item = {}
                    if output_ds:
                        excel_item['name'] = output_ds
                    if input_file:
                        excel_item['source'] = input_file
                    if input_path:
                        excel_item['path'] = input_path
                    
                    if excel_item and excel_item not in all_data[sas_name]['excel']:
                        all_data[sas_name]['excel'].append(excel_item)
            
            elif data_type == 'libname':
                libname = clean_value(row.get('libname'))
                definition = clean_value(row.get('definition'))
                if libname and definition:
                    libname_item = {
                        'name': libname.upper(),
                        'path': definition
                    }
                    if libname_item not in all_data[sas_name]['libname']:
                        all_data[sas_name]['libname'].append(libname_item)
    
    return all_data

def load_json_files():
    output_dir = 'output'
    json_files = ['daily.json', 'monthly.json', 'quarterly.json', 'on_request.json']
    
    all_json_data = {}
    
    for json_file in json_files:
        file_path = os.path.join(output_dir, json_file)
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                all_json_data[json_file] = data
                print(f"已加载 {json_file}: {len(data)} 个程序")
    
    return all_json_data

def update_json_data(json_data, excel_data):
    updated_count = 0
    matched_programs = 0
    
    for program_name, program_info in json_data.items():
        if program_name in excel_data:
            matched_programs += 1
            data = excel_data[program_name]
            
            if 'input' not in program_info:
                program_info['input'] = {}
            
            if data['imh']:
                program_info['input']['imh'] = data['imh']
            
            if data['ds']:
                program_info['input']['ds'] = data['ds']
            
            if data['excel']:
                program_info['input']['excel'] = data['excel']
            
            if data['libname']:
                program_info['libname'] = data['libname']
            
            if data['include']:
                program_info['include'] = data['include']
            
            updated_count += 1
    
    return updated_count, matched_programs

def save_json_files(json_data_dict):
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    for json_file, data in json_data_dict.items():
        file_path = os.path.join(output_dir, json_file)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"已保存到: {file_path}")

def main():
    print("=" * 80)
    print("开始处理Excel数据并更新JSON文件")
    print("=" * 80)
    
    print("\n1. 从Excel文件加载数据...")
    excel_data = load_data_from_excel()
    print(f"   共加载 {len(excel_data)} 个程序的数据")
    
    print("\n2. 加载现有JSON文件...")
    json_data_dict = load_json_files()
    
    print("\n3. 更新JSON数据...")
    total_updated = 0
    total_matched = 0
    
    for json_file, json_data in json_data_dict.items():
        updated_count, matched_count = update_json_data(json_data, excel_data)
        total_updated += updated_count
        total_matched += matched_count
        print(f"   {json_file}: 匹配 {matched_count} 个程序，更新 {updated_count} 个程序")
    
    print(f"\n   总计: 匹配 {total_matched} 个程序，更新 {total_updated} 个程序")
    
    print("\n4. 保存更新后的JSON文件...")
    save_json_files(json_data_dict)
    
    print("\n" + "=" * 80)
    print("处理完成!")
    print("=" * 80)
    
    print("\n统计信息:")
    print(f"  - Excel中提取的程序数量: {len(excel_data)}")
    print(f"  - JSON中匹配的程序数量: {total_matched}")
    print(f"  - 被更新的程序数量: {total_updated}")
    
    print("\n示例更新后的程序结构:")
    for program_name, data in excel_data.items():
        print(f"\n  {program_name}:")
        if data['imh']:
            print(f"    imh: {data['imh'][:3]}...")
        if data['ds']:
            print(f"    ds: {data['ds'][:2]}...")
        if data['excel']:
            print(f"    excel: {data['excel'][:2]}...")
        if data['libname']:
            print(f"    libname: {data['libname'][:2]}...")
        if data['include']:
            print(f"    include: {data['include'][:2]}...")
        break

if __name__ == '__main__':
    main()
