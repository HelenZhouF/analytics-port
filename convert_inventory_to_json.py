import pandas as pd
import json
import os
import numpy as np

def clean_value(value):
    if pd.isna(value) or value is None:
        return ""
    if isinstance(value, float):
        if np.isnan(value):
            return ""
        if value.is_integer():
            return int(value)
        return value
    if isinstance(value, str):
        return value.strip()
    return value

def process_sheet(excel_path, sheet_name, type_value):
    print(f"处理sheet: {sheet_name} -> type: {type_value}")
    
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    
    result = {}
    
    for idx, row in df.iterrows():
        program_raw = row.get('Program', '')
        
        if pd.isna(program_raw) or program_raw is None:
            continue
        
        program_str = str(program_raw).strip()
        
        if not program_str or program_str == 'nan' or program_str == '':
            continue
        
        if program_str.isdigit():
            continue
        
        if program_str.endswith('.txt'):
            program = program_str[:-4] + '.sas'
        elif '.' not in program_str and program_str.strip():
            program = program_str.strip() + '.sas'
        else:
            program = program_str
        
        item_no = clean_value(row.get('Item No.', ''))
        task = clean_value(row.get('Task', ''))
        path = clean_value(row.get('Path', ''))
        comments = clean_value(row.get('Comments', ''))
        is_complicated = clean_value(row.get('is_Complicated', ''))
        owner = clean_value(row.get('XX Program owner (josh/mary)', ''))
        rows = clean_value(row.get('Row in XX', ''))
        main_program = clean_value(row.get('Main Program Indicator', ''))
        
        result[program] = {
            "item_no": item_no,
            "task": task,
            "path": path,
            "comments": comments,
            "is_complicated": is_complicated,
            "owner": owner,
            "rows": rows,
            "main_program": main_program,
            "type": type_value
        }
    
    print(f"  处理了 {len(result)} 个program")
    return result

def main():
    excel_path = os.path.join('sourceData', 'Inventory_List1.xlsx')
    
    sheet_mapping = {
        'Daily -122(48219row)': 'daily',
        'Monthly-117(49653row)': 'monthly',
        'Quarterly-1(85row)': 'quarterly',
        'On request -14(2905row)': 'on_request'
    }
    
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    for sheet_name, type_value in sheet_mapping.items():
        data = process_sheet(excel_path, sheet_name, type_value)
        
        output_file = os.path.join(output_dir, f'{type_value}.json')
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        
        print(f"  已保存到: {output_file}")
        print("")

if __name__ == '__main__':
    main()
