/*
================================================================================
批量合并CSV文件程序 (最终修复版 v3)
================================================================================
功能：
1. 自动读取指定目录下的所有CSV文件
2. 为每个文件添加元数据标记（文件名、列名、列数等）
3. 将所有文件合并成一个CSV文件，便于一次导出
4. 格式设计确保可以用Python完全还原

修复说明：
- 修复了数据步骤中宏变量的使用方式
- 改进了长字符串处理，避免引号不平衡警告

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
   主宏：包含所有合并逻辑
   ================================================ */

%macro batch_merge_csv_master;
    /* 定义所有需要的宏变量 */
    %local input_dir output_file;
    %local dir_ref did num_entries i entry_name last_dot ext full_path;
    %local total_files j current_path current_name;
    
    /* ================================================
       第一步：配置参数
       ================================================ */
    
    %let input_dir = /home/u64403577/test/log/;     /* 输入目录 */
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
    
    /* 分配目录引用 */
    %let rc = %sysfunc(filename(dir_ref, &input_dir));
    
    /* 打开目录 */
    %let did = %sysfunc(dopen(&dir_ref));
    
    %if &did = 0 %then %do;
        %put 错误：无法打开目录 &input_dir;
        %put 请检查目录路径是否正确以及是否有读取权限;
        %return;
    %end;
    
    /* 获取目录中的条目数量 */
    %let num_entries = %sysfunc(dnum(&did));
    %put 目录中共有 &num_entries 个条目（文件和子目录）;
    
    /* 创建空的文件列表数据集 */
    proc sql noprint;
        create table work.csv_file_list (
            full_path char(500),
            filename char(200)
        );
    quit;
    
    /* 遍历所有条目 */
    %do i = 1 %to &num_entries;
        /* 获取条目名称 */
        %let entry_name = %sysfunc(dread(&did, &i));
        
        /* 提取文件扩展名 */
        %let last_dot = %sysfunc(findc(&entry_name, ., -%length(&entry_name)));
        
        %if &last_dot > 0 %then %do;
            %let ext = %upcase(%substr(&entry_name, &last_dot + 1));
        %end;
        %else %do;
            %let ext = ;
        %end;
        
        /* 只处理CSV文件 */
        %if &ext = CSV %then %do;
            /* 构建完整路径 */
            %let full_path = &input_dir&entry_name;
            
            %put 找到CSV文件: &entry_name;
            
            /* 添加到数据集 */
            proc sql noprint;
                insert into work.csv_file_list
                values("&full_path", "&entry_name");
            quit;
        %end;
    %end;
    
    /* 关闭目录 */
    %let rc = %sysfunc(dclose(&did));
    %let rc = %sysfunc(filename(dir_ref, ));
    
    /* 检查找到的文件数量 */
    proc sql noprint;
        select count(*) into :total_files from work.csv_file_list;
    quit;
    
    %put ================================================;
    %if &total_files = 0 %then %do;
        %put 错误：在目录 &input_dir 中未找到任何CSV文件;
        %put 请检查目录路径是否正确;
        %return;
    %end;
    %else %do;
        %put 成功找到 &total_files 个CSV文件;
    %end;
    %put ================================================;
    
    /* ================================================
       第三步：显示文件列表
       ================================================ */
    
    title '要合并的CSV文件列表';
    proc print data=work.csv_file_list label noobs;
        label 
            full_path = '完整路径'
            filename = '文件名';
    run;
    title;
    
    /* ================================================
       第四步：创建合并文件并写入文件头
       ================================================ */
    
    %put 开始创建合并文件: &output_file;
    
    data _null_;
        file "&output_file" lrecl=32767;
        
        length header_line $200;
        today_str = put(today(), yymmdd10.);
        
        /* 正确处理宏变量：使用 symget 函数 */
        total_files_num = input(symget('total_files'), 8.);
        
        header_line = '__MERGED_CSV_METADATA__|version=1.0|created=' || 
                      strip(today_str) || '|total_files=' || strip(put(total_files_num, best.));
        put header_line;
        
        put '合并文件头已写入，包含 ' total_files_num ' 个文件';
    run;
    
    /* ================================================
       第五步：将文件列表读入宏变量
       ================================================ */
    
    proc sql noprint;
        select full_path, filename into :path1-, :name1-
        from work.csv_file_list;
    quit;
    
    /* ================================================
       第六步：循环处理每个文件
       ================================================ */
    
    %put ================================================;
    %put 开始处理所有CSV文件...;
    %put ================================================;
    
    %do j = 1 %to &total_files;
        %let current_path = &&path&j;
        %let current_name = &&name&j;
        
        %put ----------------------------------------;
        %put 处理第 &j 个文件，共 &total_files 个;
        %put 处理文件: &current_name;
        %put 完整路径: &current_path;
        
        /* 步骤6.1：读取文件的第一行（表头）获取列名 */
        data work._temp_col_info;
            infile "&current_path" dlm=',' dsd truncover lrecl=32767 obs=1;
            
            /* 定义足够多的字符变量（最多200列） */
            length c1-c200 $256;
            
            /* 读取第一行 */
            input c1-c200;
            
            /* 收集列名 */
            length column_names $32767;
            array cols[*] c1-c200;
            column_names = '';
            num_cols = 0;
            
            do k = 1 to dim(cols);
                if not missing(cols[k]) then do;
                    num_cols + 1;
                    if column_names = '' then do;
                        column_names = trim(left(cols[k]));
                    end;
                    else do;
                        column_names = trim(column_names) || ',' || trim(left(cols[k]));
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
        
        /* 步骤6.2：计算数据行数 */
        %local row_count;
        %let row_count = 0;
        
        data _null_;
            infile "&current_path" truncover end=eof;
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
        
        /* 步骤6.3：写入文件开始标记 */
        data _null_;
            file "&output_file" lrecl=32767 mod;
            
            length start_tag $32767;
            
            /* 使用 symget 获取宏变量值，避免长字符串问题 */
            cols_info_val = symget('cols_info');
            col_count_val = input(symget('col_count'), 8.);
            row_count_val = input(symget('row_count'), 8.);
            current_name_val = symget('current_name');
            
            start_tag = '__FILE_START__|filename=' || trim(current_name_val) ||
                        '|column_count=' || trim(put(col_count_val, best.)) ||
                        '|row_count=' || trim(put(row_count_val, best.)) ||
                        '|columns=' || trim(cols_info_val);
            put start_tag;
        run;
        
        /* 步骤6.4：写入所有数据行（跳过表头） */
        data _null_;
            infile "&current_path" truncover lrecl=32767 firstobs=2;
            file "&output_file" lrecl=32767 mod;
            
            input;
            put _infile_;
        run;
        
        /* 步骤6.5：写入文件结束标记 */
        data _null_;
            file "&output_file" lrecl=32767 mod;
            
            length end_tag $200;
            current_name_val = symget('current_name');
            end_tag = '__FILE_END__|filename=' || trim(current_name_val);
            put end_tag;
        run;
        
        %put 文件 &current_name 处理完成;
        %put ----------------------------------------;
        
        /* 清理临时数据集 */
        proc datasets library=work nolist;
            delete _temp_col_info;
        run;
        quit;
    %end;
    
    /* ================================================
       第七步：程序结束
       ================================================ */
    
    %put ================================================;
    %put 批量合并完成！;
    %put 输出文件: &output_file;
    %put 共合并 &total_files 个CSV文件;
    %put ================================================;
    
    /* 清理临时数据集 */
    proc datasets library=work nolist;
        delete csv_file_list;
    run;
    quit;
    
    %put 程序执行结束。;
%mend batch_merge_csv_master;

/* 执行主宏 */
%batch_merge_csv_master;
