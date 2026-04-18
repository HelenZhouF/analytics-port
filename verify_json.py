import json
import os

output_dir = 'output'
json_files = ['daily.json', 'monthly.json', 'quarterly.json', 'on_request.json']

for json_file in json_files:
    file_path = os.path.join(output_dir, json_file)
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        type_value = json_file.replace('.json', '')
        
        print(f"=== {type_value} ===")
        print(f"  总数量: {len(data)}")
        if data:
            first_key = list(data.keys())[0]
            print(f"  第一个program: {first_key}")
            print(f"  示例数据:")
            for k, v in data[first_key].items():
                print(f"    {k}: {v}")
        
        print("")
