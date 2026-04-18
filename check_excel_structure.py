import pandas as pd
import os
import sys

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.stderr.reconfigure(encoding='utf-8', errors='replace')

def check_excel_structure():
    excel_file = 'sourceData/2_data_flow_information.xlsx'
    
    if not os.path.exists(excel_file):
        print(f"文件不存在: {excel_file}")
        return
    
    print("=" * 80)
    print("检查Excel文件结构")
    print("=" * 80)
    
    xls = pd.ExcelFile(excel_file)
    sheet_names = xls.sheet_names
    
    print(f"\n所有Sheet名称: {sheet_names}")
    
    for sheet in sheet_names:
        print(f"\n{'='*80}")
        print(f"Sheet: {sheet}")
        print(f"{'='*80}")
        
        df = pd.read_excel(excel_file, sheet_name=sheet)
        print(f"行数: {len(df)}, 列数: {len(df.columns)}")
        print(f"\n列名: {list(df.columns)}")
        
        required_cols = ['sas_file_name', 'Output_data_1', 'Input_data_1']
        for col in required_cols:
            if col in df.columns:
                print(f"  [OK] 找到列: {col}")
                sample_values = df[col].dropna().head(10).tolist()
                print(f"    示例值: {sample_values}")
                
                if col == 'Output_data_1':
                    has_dot = df[col].apply(lambda x: isinstance(x, str) and '.' in x).any()
                    print(f"    包含'.'的值: {'是' if has_dot else '否'}")
                    
                    dot_values = df[df[col].apply(lambda x: isinstance(x, str) and '.' in x)][col].head(10).tolist()
                    if dot_values:
                        print(f"    包含'.'的示例: {dot_values}")
            else:
                print(f"  [MISSING] 未找到列: {col}")
        
        print(f"\n前5行数据:")
        print(df.head().to_string())

if __name__ == '__main__':
    check_excel_structure()
