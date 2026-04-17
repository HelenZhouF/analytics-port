import os
import re

def parse_merged_csv(input_file, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")
    
    current_file = None
    current_columns = None
    current_rows = []
    file_count = 0
    
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    for line_num, line in enumerate(lines, 1):
        line = line.rstrip('\n')
        
        if not line:
            continue
        
        if line.startswith('__MERGED_CSV_METADATA__') or line.startswith('合并文件头已写入'):
            continue
        
        if line.startswith('__FILE_START__'):
            if current_file and current_rows:
                write_csv_file(output_dir, current_file, current_columns, current_rows)
                file_count += 1
                current_rows = []
            
            match = re.search(r'filename=([^|]+)', line)
            if match:
                current_file = match.group(1)
            
            columns_match = re.search(r'columns=([^|]+)', line)
            if columns_match:
                current_columns = columns_match.group(1)
            continue
        
        if line.startswith('__FILE_END__'):
            if current_file and current_rows:
                write_csv_file(output_dir, current_file, current_columns, current_rows)
                file_count += 1
                current_file = None
                current_columns = None
                current_rows = []
            continue
        
        if current_file:
            current_rows.append(line)
    
    if current_file and current_rows:
        write_csv_file(output_dir, current_file, current_columns, current_rows)
        file_count += 1
    
    print(f"\nSuccessfully split {file_count} CSV files into {output_dir}")
    return file_count

def write_csv_file(output_dir, filename, columns, rows):
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, 'w', encoding='utf-8', newline='') as f:
        if columns:
            f.write(columns + '\n')
        
        for row in rows:
            f.write(row + '\n')
    
    print(f"Created: {filename} ({len(rows)} rows)")

if __name__ == "__main__":
    input_file = "merged_csv_files.csv"
    output_dir = "log"
    
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' not found.")
        exit(1)
    
    parse_merged_csv(input_file, output_dir)
