/*
================================================================================
批量合并CSV文件程序 (最终版)
================================================================================
功能：
1. 自动读取指定目录下的所有CSV文件
2. 为每个文件添加元数据标记（文件名、列名、列数等）
3. 将所有文件合并成一个CSV文件，便于一次导出
4. 格式设计确保可以用Python完全还原

目录配置：
- 输入目录：/home/u64403577/test/log/
- 输出文件：/home/u64403577/test/log/merged_csv_files.csv

使用方法：
1. 确保所有CSV文件都在 /home/u64403577/test/log/ 目录下
2. 在SAS Studio中运行此程序
3. 导出生成的 merged_csv_files.csv 文件
================================================================================
*/

/* ================================================
   第一步：配置参数
   ================================================ */

%let input_dir = /home/u64403577/test/log/;     /* 输入目录，包含所有CSV文件 */
%let output_file = /home/u64403577/test/log/merged_csv_files.csv;  /* 输出文件 */

%put ================================================;
%put 批量合并CSV文件程序开始执行;
%put 输入目录: &input_dir;
%put 输出文件: &output_file;
%put ================================================;

/* ================================================
   第二步：获取目录中的所有CSV文件列表
   ================================================ */

%put 正在读取目录: &input_dir;

data work.csv_file_list;
    length full_path $500 filename $200 file_ext $10;
    
    /* 分配目录引用 */
    rc = filename('dir_ref', "&input_dir");
    
    /* 打开目录 */
    did = dopen('dir_ref');
    
    if did = 0 then do;
        put '错误：无法打开目录 ' "&input_dir";
        put '错误信息: ' sysmsg();
        stop;
    end;
    
    /* 获取目录中的文件数量 */
    num_files = dnum(did);
    put '目录中共有 ' num_files ' 个条目';
    
    /* 遍历所有条目 */
    do i = 1 to num_files;
        /* 获取条目名称 */
        entry_name = dread(did, i);
        
        /* 提取文件扩展名 */
        last_dot = findc(entry_name, '.', -length(entry_name));
        if last_dot > 0 then do;
            file_ext = upcase(substr(entry_name, last_dot + 1));
        end;
        else do;
            file_ext = '';
        end;
        
        /* 只处理CSV文件 */
        if file_ext = 'CSV' then do;
            /* 构建完整路径 */
            full_path = "&input_dir" || strip(entry_name);
            filename = entry_name;
            
            output;
            put '找到CSV文件: ' entry_name;
        end;
    end;
    
    /* 关闭目录 */
    rc = dclose(did);
    rc = filename('dir_ref', '');
    
    keep full_path filename;
run;

/* 检查找到的文件数量 */
proc sql noprint;
    select count(*) into :total_files from work.csv_file_list;
quit;

%put ================================================;
%if &total_files = 0 %then %do;
    %put 错误：在目录 &input_dir 中未找到任何CSV文件;
    %put 请检查目录路径是否正确;
%end;
%else %do;
    %put 成功找到 &total_files 个CSV文件;
%end;
%put ================================================;

/* 显示文件列表 */
%if &total_files > 0 %then %do;
    title '要合并的CSV文件列表';
    proc print data=work.csv_file_list label noobs;
        label 
            full_path = '完整路径'
            filename = '文件名';
    run;
    title;
%end;

/* ================================================
   第三步：定义处理单个文件的宏
   ================================================ */

%macro process_csv_file(file_path=, file_name=);
    %if &file_path = %str() %then %return;
    
    %put ----------------------------------------;
    %put 处理文件: &file_name;
    %put 完整路径: &file_path;
    
    /* 步骤1：读取文件的第一行（表头）获取列名 */
    data work._temp_col_info;
        infile "&file_path" dlm=',' dsd truncover lrecl=32767 obs=1;
        
        /* 定义足够多的字符变量（最多200列） */
        length c1-c200 $256;
        
        /* 读取第一行 */
        input c1-c200;
        
        /* 收集列名 */
        length column_names $32767;
        array cols[*] c1-c200;
        column_names = '';
        num_cols = 0;
        
        do j = 1 to dim(cols);
            if not missing(cols[j]) then do;
                num_cols + 1;
                if column_names = '' then do;
                    column_names = trim(left(cols[j]));
                end;
                else do;
                    column_names = trim(column_names) || ',' || trim(left(cols[j]));
                end;
            end;
        end;
        
        keep column_names num_cols;
    run;
    
    /* 获取列名和列数 */
    %local cols_info col_count;
    
    proc sql noprint;
        select column_names, num_cols into :cols_info, :col_count
        from work._temp_col_info;
    quit;
    
    %if &sqlobs = 0 %then %do;
        %let cols_info = ;
        %let col_count = 0;
    %end;
    
    %put 列数: &col_count;
    %put 列名: &cols_info;
    
    /* 步骤2：计算数据行数 */
    %local row_count;
    %let row_count = 0;
    
    data _null_;
        infile "&file_path" truncover end=eof;
        input;
        
        /* 跳过表头行 */
        if _n_ > 1 then do;
            rcnt + 1;
        end;
        
        if eof then do;
            call symputx('row_count', rcnt);
        end;
    run;
    
    %put 数据行数: &row_count;
    
    /* 步骤3：写入文件开始标记 */
    data _null_;
        file "&output_file" lrecl=32767 mod;
        
        length start_tag $32767;
        start_tag = '__FILE_START__|filename=' || trim("&file_name") || 
                    '|column_count=' || trim(put(&col_count, best.)) ||
                    '|row_count=' || trim(put(&row_count, best.)) ||
                    '|columns=' || trim("&cols_info");
        put start_tag;
    run;
    
    /* 步骤4：写入所有数据行（跳过表头） */
    data _null_;
        infile "&file_path" truncover lrecl=32767 firstobs=2;
        file "&output_file" lrecl=32767 mod;
        
        input;
        put _infile_;
    run;
    
    /* 步骤5：写入文件结束标记 */
    data _null_;
        file "&output_file" lrecl=32767 mod;
        
        length end_tag $200;
        end_tag = '__FILE_END__|filename=' || trim("&file_name");
        put end_tag;
    run;
    
    %put 文件 &file_name 处理完成;
    %put ----------------------------------------;
    
    /* 清理临时数据集 */
    proc datasets library=work nolist;
        delete _temp_col_info;
    run;
    quit;
%mend process_csv_file;

/* ================================================
   第四步：创建合并文件并写入文件头
   ================================================ */

%if &total_files > 0 %then %do;
    %put 开始创建合并文件: &output_file;
    
    data _null_;
        file "&output_file" lrecl=32767;
        
        length header_line $200;
        today_str = put(today(), yymmdd10.);
        header_line = '__MERGED_CSV_METADATA__|version=1.0|created=' || 
                      strip(today_str) || '|total_files=' || strip(put(&total_files, best.));
        put header_line;
        
        put '合并文件头已写入，包含 ' &total_files ' 个文件';
    run;
%end;

/* ================================================
   第五步：遍历文件列表并处理每个文件
   ================================================ */

%if &total_files > 0 %then %do;
    %put ================================================;
    %put 开始处理所有CSV文件...;
    %put ================================================;
    
    data _null_;
        set work.csv_file_list;
        
        length macro_call $1000;
        macro_call = '%process_csv_file(file_path=' || trim(full_path) || 
                     ', file_name=' || trim(filename) || ');';
        
        put '执行: ' macro_call;
        call execute(macro_call);
    run;
%end;

/* ================================================
   第六步：程序结束
   ================================================ */

%put ================================================;
%if &total_files > 0 %then %do;
    %put 批量合并完成！;
    %put 输出文件: &output_file;
    %put 共合并 &total_files 个CSV文件;
%end;
%else %do;
    %put 程序未执行任何合并操作;
%end;
%put ================================================;

/* 清理临时数据集 */
proc datasets library=work nolist;
    delete csv_file_list;
run;
quit;

%put 程序执行结束。;
