import pandas as pd
import json
import os

# 读取Excel文件
excel_path = os.path.join('sourceData', 'Inventory_List1.xlsx')

# 获取所有sheet名称
xl = pd.ExcelFile(excel_path)
print("=== Excel Sheet 名称 ===")
for sheet_name in xl.sheet_names:
    print(f"  - {sheet_name}")
    # 读取每个sheet的前几行
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    print(f"    行数: {len(df)}")
    print(f"    列名: {df.columns.tolist()}")
    if len(df) > 0:
        print(f"    前3行:")
        print(df.head(3))
    print("\n")
