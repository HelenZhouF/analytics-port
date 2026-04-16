/*
================================================================================
批量合并CSV文件程序 (修复版)
================================================================================
功能：
1. 读取指定目录下的所有CSV文件（或手动指定文件列表）
2. 为每个文件添加元数据标记（文件名、列名、列数等）
3. 将所有文件合并成一个CSV文件，便于一次导出
4. 格式设计确保可以用Python完全还原

修复内容：
1. 使用SAS内置目录函数替代PIPE命令，避免权限问题
2. 将整个程序包装在宏中，解决%GOTO在开放代码中无效的问题
3. 提供两种使用方式：自动获取目录文件或手动指定文件列表

使用方法：
方式1（自动获取目录文件）：
   - 修改 input_path 为你的CSV文件所在目录
   - 修改 output_file 为合并后的输出文件路径
   - 设置 use_file_list = 0

方式2（手动指定文件列表）：
   - 设置 use_file_list = 1
   - 在 file_list 数据集中手动添加要合并的文件
   - 修改 output_file 为合并后的输出文件路径
================================================================================
*/

/* ================================================
   第一部分：用户配置区域
   ================================================ */

%macro batch_merge_csv;
    /* --------------------------
       请修改以下配置参数
       -------------------------- */
    
    /* 方式1：自动获取目录文件的配置 */
    %let input_path = /home/u64403577/test/;      /* 输入目录，包含所有要合并的CSV文件 */
    %let output_file = /home/u64403577/test/merged_csv_files.csv;  /* 合并后的输出文件 */
    %let file_pattern = *.csv;                     /* 要合并的文件模式（仅用于显示，实际使用SAS目录函数） */
    
    /* 方式2：手动指定文件列表的配置 */
    %let use_file_list = 0;                         /* 1=使用手动指定的文件列表，0=自动获取目录文件 */
    
    /* --------------------------
       配置结束
       -------------------------- */

    %put ================================================;
    %put 批量合并CSV文件程序开始执行;
    %put ================================================;

    /* ================================================
       第二部分：获取CSV文件列表
       ================================================ */
    
    /* 创建文件列表数据集 */
    data work.csv_files;
        length full_path $256 filename $100;
        stop;  /* 先创建空结构 */
    run;

    %if &use_file_list = 1 %then %do;
        /* 方式2：使用手动指定的文件列表 */
        %put 使用手动指定的文件列表模式;
        %put 请在下面的数据步骤中添加你的文件路径;
        
        /* 示例：手动添加文件（用户需要根据实际情况修改） */
        data work.csv_files;
            length full_path $256 filename $100;
            
            /* 在这里添加你的文件，每一行一个文件 */
            /* 示例：
            full_path = '/home/yourname/file1.csv';
            filename = scan(full_path, -1, '/');
            output;
            
            full_path = '/home/yourname/file2.csv';
            filename = scan(full_path, -1, '/');
            output;
            */
            
            /* 注意：如果没有手动添加文件，程序会提示错误 */
            stop;
        run;
        
        /* 检查是否有手动添加的文件 */
        proc sql noprint;
            select count(*) into :manual_file_count from work.csv_files;
        quit;
        
        %if &manual_file_count = 0 %then %do;
            %put 错误：手动指定文件列表模式已启用，但未添加任何文件;
            %put 请在 data work.csv_files 数据步骤中添加你的文件路径;
            %goto end_macro;
        %end;
        
        %put 已手动指定 &manual_file_count 个文件;
    %end;
    %else %do;
        /* 方式1：使用SAS内置目录函数自动获取文件列表 */
        %put 使用自动获取目录文件模式;
        %put 正在读取目录: &input_path;
        
        /* 分配目录引用 */
        filename dirref "&input_path";
        
        /* 打开目录 */
        %let did = %sysfunc(dopen(dirref));
        
        %if &did = 0 %then %do;
            %put 错误：无法打开目录 &input_path;
            %put 错误信息: %sysfunc(sysmsg());
            %goto end_macro;
        %end;
        
        /* 获取目录中的文件数量 */
        %let file_count = %sysfunc(dnum(&did));
        %put 目录中共有 &file_count 个条目（文件和子目录）;
        
        /* 遍历目录中的所有条目 */
        %do i = 1 %to &file_count;
            /* 获取条目名称 */
            %let entry_name = %sysfunc(dread(&did, &i));
            
            /* 检查是否是CSV文件（不区分大小写） */
            %if %upcase(%scan(&entry_name, -1, .)) = CSV %then %do;
                /* 构建完整路径 */
                %let full_path = &input_path&entry_name;
                
                /* 添加到文件列表数据集 */
                proc sql noprint;
                    insert into work.csv_files
                    values("&full_path", "&entry_name");
                quit;
                
                %put 找到CSV文件: &entry_name;
            %end;
        %end;
        
        /* 关闭目录 */
        %let rc = %sysfunc(dclose(&did));
        filename dirref clear;
        
        %put 目录读取完成;
    %end;

    /* 检查是否找到文件 */
    proc sql noprint;
        select count(*) into :total_files from work.csv_files;
    quit;

    %if &total_files = 0 %then %do;
        %put 错误：未找到任何CSV文件;
        %if &use_file_list = 1 %then %do;
            %put 请检查是否在手动文件列表中添加了文件;
        %end;
        %else %do;
            %put 请检查目录路径是否正确: &input_path;
        %end;
        %goto end_macro;
    %end;

    %put ================================================;
    %put 共找到 &total_files 个CSV文件，准备合并;
    %put ================================================;

    /* 显示文件列表 */
    %put 文件列表:;
    proc sql noprint;
        select filename into :file1 - :file&total_files from work.csv_files;
    quit;
    
    %do i = 1 %to &total_files;
        %put  &i.. &&file&i;
    %end;

    /* ================================================
       第三部分：创建合并文件并写入元数据头部
       ================================================ */

    %put ================================================;
    %put 开始创建合并文件: &output_file;
    %put ================================================;

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
                            'total_files=' || strip(put(&total_files, best.)));
        put header_line;
        
        put '合并文件头已写入';
    run;

    /* ================================================
       第四部分：创建处理单个文件的宏
       ================================================ */

    %macro process_file(full_path=, filename=);
        %put 正在处理文件: &filename;
        
        /* 第一步：读取文件的第一行（列名） */
        data work._temp_header_;
            infile "&full_path" dlm=',' dsd truncover lrecl=32767 obs=1;
            input col1-col200 $256.;  /* 增加到200列 */
            
            /* 收集所有非空列名 */
            length columns $32767;
            array cols[*] col1-col200;
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
            
            put '文件开始标记已写入: ' "&filename";
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
            
            put '文件结束标记已写入: ' "&filename";
            put '文件 ' "&filename" ' 处理完成';
        run;
        
        /* 清理临时数据集 */
        proc datasets library=work nolist;
            delete _temp_header_;
        run;
        quit;
    %mend process_file;

    /* ================================================
       第五部分：遍历所有文件并合并
       ================================================ */

    %put ================================================;
    %put 开始合并文件...;
    %put ================================================;

    /* 使用数据步骤遍历文件列表并调用处理宏 */
    data _null_;
        set work.csv_files;
        length macro_call $500;
        macro_call = cats('%process_file(full_path=', full_path, ', filename=', filename, ');');
        call execute(macro_call);
    run;

    /* ================================================
       第六部分：程序结束
       ================================================ */

    %end_macro:

    %put ================================================;
    %if &total_files > 0 %then %do;
        %put 批量合并完成！;
        %put 合并文件: &output_file;
        %put 共合并 &total_files 个文件;
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

%mend batch_merge_csv;

/* 执行宏 */
%batch_merge_csv;
