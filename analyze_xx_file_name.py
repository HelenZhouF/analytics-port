import pandas as pd
import os

excel_files = {
    "include": "1_include_relationship.xlsx",
    "imh": "2_input_database_tables.xlsx",
    "ds": "3_input_library_data.xlsx",
    "excel": "4_sas_imported_files_output.xlsx",
    "libname": "0.2_sas_path_definition.xlsx"
}

sheets = {
    "include": "Include_Relationship",
    "imh": "imh_input_tables",
    "ds": "libraries_source_data",
    "excel": "Imported Files",
    "libname": "libname_definition"
}

source_dir = 'sourceData'

for data_type, excel_file in excel_files.items():
    file_path = os.path.join(source_dir, excel_file)
    sheet_name = sheets[data_type]
    
    if not os.path.exists(file_path):
        print(f"文件不存在: {file_path}")
        continue
    
    print(f"\n{'='*80}")
    print(f"数据类型: {data_type}")
    print(f"文件: {excel_file}")
    print(f"Sheet: {sheet_name}")
    print(f"{'='*80}")
    
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    
    print(f"\n总数据行: {len(df)}")
    print(f"\n列名: {df.columns.tolist()}")
    
    print(f"\nXX_file_name 列的前20个值:")
    for i, val in enumerate(df['XX_file_name'].head(20).tolist()):
        print(f"  [{i+1}] {val} (类型: {type(val)})")
    
    print(f"\n去重后的 XX_file_name 数量: {df['XX_file_name'].nunique()}")
    
    print(f"\n主要字段示例:")
    if data_type == 'include':
        print(df[['XX_file_name', 'Included_File_Name']].head(10))
    elif data_type == 'imh':
        print(df[['XX_file_name', 'Input_data_1']].head(10))
    elif data_type == 'ds':
        print(df[['XX_file_name', 'Library_Name', 'Dataset_Name']].head(10))
    elif data_type == 'excel':
        print(df[['XX_file_name', 'output_dataset_name', 'input_file_name', 'input_file_path']].head(10))
    elif data_type == 'libname':
        print(df[['XX_file_name', 'libname', 'definition']].head(10))
