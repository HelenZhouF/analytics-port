/*
================================================================================
批量合并CSV文件程序 (最终修复版)
================================================================================
功能：
1. 手动指定要合并的CSV文件列表（最可靠的方式）
2. 为每个文件添加元数据标记（文件名、列名、列数等）
3. 将所有文件合并成一个CSV文件，便于一次导出
4. 格式设计确保可以用Python完全还原

重要说明：
由于SAS Studio环境限制，此版本使用手动指定文件列表的方式，
这是最可靠、最不会遇到权限问题的方法。

使用方法：
1. 在 file_list 数据集中手动添加要合并的文件路径
2. 修改 output_file 为合并后的输出文件路径
3. 在SAS Studio中运行此程序
================================================================================
*/

/* ================================================
   第一步：用户配置 - 请修改这里
   ================================================ */

/* 配置：合并后的输出文件路径 */
%let output_file = /home/u64403577/test/merged_csv_files.csv;

/* ================================================
   第二步：手动指定要合并的文件列表
   ================================================ */
/* 
   请在下面的数据步骤中添加你的CSV文件路径
   每行一个文件，格式如下：
   
   full_path = '/完整/路径/到/你的/文件1.csv';
   output;
   
   full_path = '/完整/路径/到/你的/文件2.csv';
   output;
   
   ... 添加更多文件 ...
*/

data work.file_list;
    length full_path $500 filename $200;
    
    /* 示例文件 - 请替换为你的实际文件路径 */
    /* 注意：路径必须是SAS Studio可以访问的完整路径 */
    
    /* 示例1：取消下面这行的注释并修改路径 */
    * full_path = '/home/u64403577/test/sales_data.csv';
    
    /* 示例2：添加更多文件 */
    * full_path = '/home/u64403577/test/customer_data.csv';
    * output;
    
    /* ====== 在这里添加你的文件 ====== */
    
    /* 例如：
    full_path = '/home/你的用户名/你的目录/file1.csv';
    output;
    
    full_path = '/home/你的用户名/你的目录/file2.csv';
    output;
    
    full_path = '/home/你的用户名/你的目录/file3.csv';
    output;
    */
    
    /* ====== 文件添加结束 ====== */
    
    /* 提取文件名（从完整路径中） */
    if not missing(full_path) then do;
        /* 查找最后一个斜杠的位置 */
        last_slash = findc(full_path, '/', -length(full_path));
        if last_slash > 0 then do;
            filename = substr(full_path, last_slash + 1);
        end;
        else do;
            /* 如果没有斜杠，整个路径就是文件名 */
            filename = full_path;
        end;
        
        /* 确保是CSV文件 */
        if upcase(scan(filename, -1, '.')) = 'CSV' then do;
            output;
            put '已添加文件: ' full_path;
        end;
        else do;
            put '警告：跳过非CSV文件: ' full_path;
        end;
    end;
    
    keep full_path filename;
run;

/* 检查文件列表 */
proc sql noprint;
    select count(*) into :file_count from work.file_list;
quit;

%put ================================================;
%if &file_count = 0 %then %do;
    %put 错误：未找到任何CSV文件！;
    %put 请在 data work.file_list 数据步骤中添加你的文件路径;
    %put 程序结束。;
    %goto end_program;
%end;
%else %do;
    %put 成功添加 &file_count 个CSV文件，准备合并;
%end;
%put ================================================;

/* 显示文件列表 */
title '要合并的文件列表';
proc print data=work.file_list label noobs;
    label 
        full_path = '完整路径'
        filename = '文件名';
run;
title;

/* ================================================
   第三步：创建合并文件并写入元数据头部
   ================================================ */

%put 开始创建合并文件: &output_file;

data _null_;
    file "&output_file" lrecl=32767;
    
    /* 写入合并文件的元数据头部 */
    /* 格式：__MERGED_CSV_METADATA__|version=1.0|created=YYYY-MM-DD|total_files=N */
    length header_line $200;
    today_char = put(today(), yymmdd10.);
    file_count_char = put(&file_count, best.);
    header_line = '__MERGED_CSV_METADATA__|version=1.0|created=' || 
                   strip(today_char) || '|total_files=' || strip(file_count_char);
    put header_line;
    
    put '合并文件头已写入，包含 ' file_count_char ' 个文件';
run;

/* ================================================
   第四步：定义处理单个文件的宏
   ================================================ */

%macro process_single_csv(full_path=, filename=);
    %put ----------------------------------------;
    %put 正在处理文件: &filename;
    %put 完整路径: &full_path;
    
    /* 步骤1：读取文件的第一行（表头）来获取列名 */
    data work._temp_header;
        infile "&full_path" dlm=',' dsd truncover lrecl=32767 obs=1;
        
        /* 定义足够多的字符变量来存储列名（最多200列） */
        length c1-c200 $256;
        
        /* 读取第一行 */
        input c1-c200;
        
        /* 收集所有非空列名 */
        length all_columns $32767;
        array col_names[*] c1-c200;
        all_columns = '';
        col_count = 0;
        
        do i = 1 to dim(col_names);
            if not missing(col_names[i]) then do;
                col_count + 1;
                if all_columns = '' then do;
                    all_columns = trim(left(col_names[i]));
                end;
                else do;
                    all_columns = trim(all_columns) || ',' || trim(left(col_names[i]));
                end;
            end;
        end;
        
        /* 保存结果 */
        keep all_columns col_count;
    run;
    
    /* 获取列名和列数 */
    %local file_columns file_col_count;
    
    proc sql noprint;
        select all_columns, col_count into :file_columns, :file_col_count
        from work._temp_header;
    quit;
    
    /* 处理空值情况 */
    %if &sqlobs = 0 %then %do;
        %let file_columns = ;
        %let file_col_count = 0;
    %end;
    
    %put 文件 &filename 有 &file_col_count 列;
    %put 列名: &file_columns;
    
    /* 步骤2：计算数据行数（不包括表头） */
    %local file_row_count;
    %let file_row_count = 0;
    
    data _null_;
        infile "&full_path" truncover end=eof;
        input;
        
        /* 第一行是表头，跳过 */
        if _n_ > 1 then do;
            row_count + 1;
        end;
        
        if eof then do;
            call symputx('file_row_count', row_count);
        end;
    run;
    
    %put 文件 &filename 有 &file_row_count 行数据;
    
    /* 步骤3：写入文件开始标记到合并文件 */
    data _null_;
        file "&output_file" lrecl=32767 mod;
        
        length start_line $32767;
        start_line = '__FILE_START__|filename=' || trim("&filename") || 
                     '|column_count=' || trim(put(&file_col_count, best.)) ||
                     '|row_count=' || trim(put(&file_row_count, best.)) ||
                     '|columns=' || trim("&file_columns");
        put start_line;
        
        put '已写入文件开始标记: ' "&filename";
    run;
    
    /* 步骤4：读取并写入所有数据行（跳过表头） */
    data _null_;
        infile "&full_path" truncover lrecl=32767 firstobs=2;
        file "&output_file" lrecl=32767 mod;
        
        input;
        put _infile_;  /* 直接写入原始行 */
    run;
    
    %put 已写入 &file_row_count 行数据;
    
    /* 步骤5：写入文件结束标记 */
    data _null_;
        file "&output_file" lrecl=32767 mod;
        
        length end_line $200;
        end_line = '__FILE_END__|filename=' || trim("&filename");
        put end_line;
        
        put '已写入文件结束标记: ' "&filename";
        put '文件 ' "&filename" ' 处理完成';
    run;
    
    /* 清理临时数据集 */
    proc datasets library=work nolist;
        delete _temp_header;
    run;
    quit;
    
    %put ----------------------------------------;
%mend process_single_csv;

/* ================================================
   第五步：遍历文件列表并处理每个文件
   ================================================ */

%put ================================================;
%put 开始合并所有文件...;
%put ================================================;

/* 使用数据步骤生成宏调用 */
data _null_;
    set work.file_list;
    
    length macro_call $1000;
    macro_call = '%process_single_csv(full_path=' || trim(full_path) || 
                 ', filename=' || trim(filename) || ');';
    
    put '执行: ' macro_call;
    call execute(macro_call);
run;

/* ================================================
   第六步：程序结束
   ================================================ */

%end_program:

%put ================================================;
%if &file_count > 0 %then %do;
    %put 批量合并完成！;
    %put 输出文件: &output_file;
    %put 共合并 &file_count 个文件;
%end;
%else %do;
    %put 程序未执行任何合并操作;
%end;
%put ================================================;

/* 清理临时数据集 */
proc datasets library=work nolist;
    delete file_list;
run;
quit;

%put 程序执行结束。;
