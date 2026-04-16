/*
================================================================================
批量合并CSV文件程序
================================================================================
功能：
1. 读取指定目录下的所有CSV文件
2. 为每个文件添加元数据标记（文件名、列名、列数等）
3. 将所有文件合并成一个CSV文件，便于一次导出
4. 格式设计确保可以用Python完全还原

使用方法：
1. 修改 input_path 为你的CSV文件所在目录
2. 修改 output_file 为合并后的输出文件路径
3. 在SAS Studio中运行此程序
================================================================================
*/

/* ================================================
   第一部分：设置宏变量和环境
   ================================================ */

%let input_path = /home/u64403577/test/;      /* 输入目录，包含所有要合并的CSV文件 */
%let output_file = /home/u64403577/test/merged_csv_files.csv;  /* 合并后的输出文件 */
%let file_pattern = *.csv;                     /* 要合并的文件模式 */

%put ================================================;
%put 批量合并CSV文件程序开始执行;
%put 输入目录: &input_path;
%put 输出文件: &output_file;
%put ================================================;

/* ================================================
   第二部分：获取目录下所有CSV文件列表
   ================================================ */

/* 使用FILENAME管道获取文件列表 */
filename dirlist pipe "ls &input_path.&file_pattern";

data work.csv_files;
    infile dirlist truncover;
    input full_path $256.;
    
    /* 提取文件名（不含路径） */
    filename = scan(full_path, -1, '/');
    
    /* 只保留.csv文件 */
    if upcase(scan(filename, -1, '.')) = 'CSV' then output;
    
    label 
        full_path = '完整路径'
        filename = '文件名';
run;

/* 检查是否找到文件 */
proc sql noprint;
    select count(*) into :file_count from work.csv_files;
quit;

%if &file_count = 0 %then %do;
    %put 错误：在目录 &input_path 中未找到CSV文件;
    %goto end_program;
%end;

%put 找到 &file_count 个CSV文件，准备合并...;

/* ================================================
   第三部分：创建合并文件并写入元数据头部
   ================================================ */

/* 打开输出文件并写入文件头 */
data _null_;
    file "&output_file" dlm=',' dsd lrecl=32767;
    
    /* 写入合并文件的元数据头部 */
    /* 格式：__MERGED_CSV_METADATA__|version=1.0|created=YYYY-MM-DD|total_files=N */
    length header_line $200;
    today = put(today(), yymmdd10.);
    header_line = catx('|', '__MERGED_CSV_METADATA__', 
                        'version=1.0', 
                        'created=' || strip(today),
                        'total_files=' || strip(put(&file_count, best.)));
    put header_line;
    
    %put 合并文件头已写入;
run;

/* ================================================
   第四部分：遍历每个CSV文件并合并
   ================================================ */

/* 创建宏来处理单个文件 */
%macro process_single_file(full_path=, filename=);
    %put 正在处理文件: &filename;
    
    /* 第一步：读取文件的第一行（列名） */
    data work._temp_header_;
        infile "&full_path" dlm=',' dsd truncover lrecl=32767 obs=1;
        input col1-col100 $256.;  /* 假设最多100列，根据实际情况调整 */
        
        /* 收集所有非空列名 */
        length columns $32767;
        array cols[*] col1-col100;
        columns = '';
        do i = 1 to dim(cols);
            if not missing(cols[i]) then do;
                if columns = '' then columns = trim(cols[i]);
                else columns = catx(',', columns, trim(cols[i]));
            end;
        end;
        
        /* 计算列数 */
        column_count = countw(columns, ',');
        
        keep columns column_count;
    run;
    
    /* 获取列名和列数 */
    proc sql noprint;
        select columns, column_count into :file_columns, :file_col_count
        from work._temp_header_;
    quit;
    
    %put 文件 &filename 有 &file_col_count 列;
    %put 列名: &file_columns;
    
    /* 第二步：计算数据行数（不包括表头） */
    data _null_;
        infile "&full_path" truncover end=eof;
        input;
        if _n_ = 1 then delete;  /* 跳过表头 */
        row_count + 1;
        if eof then do;
            call symputx('file_row_count', row_count);
        end;
    run;
    
    %if &syserr > 4 %then %let file_row_count = 0;
    %put 文件 &filename 有 &file_row_count 行数据;
    
    /* 第三步：写入文件开始标记到合并文件 */
    data _null_;
        file "&output_file" dlm=',' dsd lrecl=32767 mod;  /* mod=追加模式 */
        
        /* 写入文件开始标记 */
        /* 格式：__FILE_START__|filename=xxx.csv|column_count=N|row_count=M|columns=col1,col2... */
        length start_line $32767;
        start_line = catx('|', '__FILE_START__',
                          'filename=' || strip("&filename"),
                          'column_count=' || strip(put(&file_col_count, best.)),
                          'row_count=' || strip(put(&file_row_count, best.)),
                          'columns=' || strip("&file_columns"));
        put start_line;
        
        %put 文件开始标记已写入: &filename;
    run;
    
    /* 第四步：读取并写入所有数据行（跳过表头） */
    data _null_;
        infile "&full_path" dlm=',' dsd truncover lrecl=32767 firstobs=2;
        file "&output_file" dlm=',' dsd lrecl=32767 mod;
        
        /* 动态处理所有列 */
        input;
        put _infile_;  /* 直接写入原始行，保持原有格式 */
    run;
    
    /* 第五步：写入文件结束标记 */
    data _null_;
        file "&output_file" dlm=',' dsd lrecl=32767 mod;
        
        /* 写入文件结束标记 */
        /* 格式：__FILE_END__|filename=xxx.csv */
        length end_line $200;
        end_line = catx('|', '__FILE_END__',
                        'filename=' || strip("&filename"));
        put end_line;
        
        %put 文件结束标记已写入: &filename;
        %put 文件 &filename 处理完成;
    run;
    
    /* 清理临时数据集 */
    proc datasets library=work nolist;
        delete _temp_header_;
    run;
    quit;
%mend process_single_file;

/* 遍历所有文件 */
data _null_;
    set work.csv_files;
    call execute('%process_single_file(full_path=' || trim(full_path) || ', filename=' || trim(filename) || ');');
run;

/* ================================================
   第五部分：程序结束
   ================================================ */

%end_program:

%put ================================================;
%if &file_count > 0 %then %do;
    %put 批量合并完成！;
    %put 合并文件: &output_file;
    %put 共合并 &file_count 个文件;
%end;
%else %do;
    %put 程序未执行任何合并操作;
%end;
%put ================================================;

/* 清理临时数据集 */
proc datasets library=work nolist;
    delete csv_files;
run;
quit;
