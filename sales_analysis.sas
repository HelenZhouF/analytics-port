/*
================================================================================
SAS Studio 综合演示程序
================================================================================
程序功能：
1. 从CSV文件读取销售数据
2. 数据清洗和转换 (Data Step)
3. 数据汇总分析 (Proc SQL)
4. 数据排序 (Proc Sort)
5. 宏变量和宏程序
6. 导出结果到CSV (Proc Export)

输入文件：/home/u64403577/test/sales_data.csv
输出文件：/home/u64403577/test/sales_analysis_result.csv
================================================================================
*/

/* ================================================
   第一部分：设置宏变量和环境
   ================================================ */

%let input_path = /home/u64403577/test/;
%let output_path = /home/u64403577/test/;
%let input_file = &input_path.sales_data.csv;
%let output_file = &output_path.sales_analysis_result.csv;

%put ================================================;
%put 程序开始执行;
%put 输入文件: &input_file;
%put 输出文件: &output_file;
%put ================================================;

/* ================================================
   第二部分：定义宏程序
   ================================================ */

%macro calculate_stats(dataset=, var=, group_var=);
    /*
    宏功能：计算指定变量的统计量
    参数：
        dataset: 输入数据集
        var: 需要计算统计量的变量
        group_var: 分组变量
    */
    
    proc sql noprint;
        create table &dataset._stats as
        select 
            &group_var,
            count(*) as total_orders,
            sum(&var) as total_&var,
            mean(&var) as avg_&var format=10.2,
            min(&var) as min_&var,
            max(&var) as max_&var,
            std(&var) as std_&var format=10.2
        from &dataset
        group by &group_var
        order by total_&var desc;
    quit;
    
    %put 统计计算完成: &dataset._stats;
%mend calculate_stats;

%macro print_summary(dataset=, title=);
    /*
    宏功能：打印数据集摘要信息
    参数：
        dataset: 输入数据集
        title: 标题
    */
    
    title "&title";
    proc contents data=&dataset varnum;
    run;
    
    proc print data=&dataset(obs=10) label;
        label 
            order_id = '订单ID'
            order_date = '订单日期'
            region = '地区'
            product = '产品'
            quantity = '数量'
            unit_price = '单价'
            discount = '折扣率'
            total_amount = '总金额'
            discounted_amount = '折扣后金额';
    run;
    title;
%mend print_summary;

%macro export_to_csv(dataset=, columns=);
    /*
    宏功能：将数据集导出为CSV文件
    参数：
        dataset: 输入数据集名称
        columns: 可选参数，指定要导出的列，多个列用空格分隔（推荐）或逗号分隔
                 如果使用逗号分隔，需要用 %str() 引用，例如：columns=%str(make,model)
                 如果不指定 columns，则导出所有列
    */
    
    %let log_path = /home/u64403577/test/log;
    
    %local rc did;
    %let rc = %sysfunc(filename(ref, &log_path));
    %let did = %sysfunc(dopen(&ref));
    %if &did = 0 %then %do;
        %put 日志目录不存在，正在创建: &log_path;
        %sysexec mkdir -p &log_path;
    %end;
    %else %do;
        %let rc = %sysfunc(dclose(&did));
    %end;
    %let rc = %sysfunc(filename(ref));
    
    %local dsname;
    %if %index(&dataset, .) > 0 %then %do;
        %let dsname = %scan(&dataset, 2, .);
    %end;
    %else %do;
        %let dsname = &dataset;
    %end;
    
    %let outfile = &log_path/&dsname..csv;
    
    %put 正在导出数据集 &dataset 到 &outfile;
    
    %if %length(&columns) > 0 %then %do;
        %let columns = %sysfunc(translate(&columns, ' ', ','));
        
        data work._temp_export_;
            set &dataset;
            keep &columns;
        run;
        
        proc export data=work._temp_export_
            outfile="&outfile"
            dbms=csv
            replace;
            delimiter=',';
            putnames=yes;
        run;
        
        proc datasets library=work nolist;
            delete _temp_export_;
        run;
        quit;
    %end;
    %else %do;
        proc export data=&dataset
            outfile="&outfile"
            dbms=csv
            replace;
            delimiter=',';
            putnames=yes;
        run;
    %end;
    
    %put 数据集 &dataset 已成功导出到 &outfile;
%mend export_to_csv;

/* ================================================
   第三部分：读取CSV数据 (Data Step)
   ================================================ */

%put 开始读取CSV数据...;

data work.raw_sales;
    infile "&input_file" 
        dlm=',' 
        firstobs=2 
        dsd 
        truncover
        lrecl=32767;
    
    informat 
        order_id 8.
        order_date yymmdd10.
        region $20.
        product $30.
        quantity 8.
        unit_price 8.2
        discount 8.4
        customer_id $10.
        customer_name $50.
        age 8.
        gender $10.
        membership_level $20.;
    
    format 
        order_id 8.
        order_date yymmdd10.
        region $20.
        product $30.
        quantity 8.
        unit_price dollar10.2
        discount percent8.2
        customer_id $10.
        customer_name $50.
        age 8.
        gender $10.
        membership_level $20.;
    
    input 
        order_id
        order_date
        region $
        product $
        quantity
        unit_price
        discount
        customer_id $
        customer_name $
        age
        gender $
        membership_level $;
    
    /* 数据验证标记 */
    if missing(order_id) then delete;
    if quantity < 0 then quantity = 0;
    if unit_price < 0 then unit_price = 0;
    if discount < 0 then discount = 0;
    if discount > 1 then discount = 1;
    
    /* 计算订单月份 */
    order_month = put(order_date, yymmd7.);
    
    /* 计算年龄分组 */
    if age < 30 then age_group = '18-29';
    else if age < 40 then age_group = '30-39';
    else if age < 50 then age_group = '40-49';
    else age_group = '50+';
    
    /* 计算订单价值分类 */
    order_value = quantity * unit_price;
    if order_value < 500 then value_category = '低价值';
    else if order_value < 2000 then value_category = '中价值';
    else value_category = '高价值';
    
    label 
        order_month = '订单月份'
        age_group = '年龄分组'
        order_value = '订单价值'
        value_category = '价值分类';
run;

%put 原始数据读取完成，观测数: &sysnobs;
%export_to_csv(dataset=work.raw_sales);

/* ================================================
   第四部分：数据清洗和转换 (Data Step)
   ================================================ */

%put 开始数据清洗和转换...;

data work.clean_sales;
    set work.raw_sales;
    
    /* 计算总金额和折扣后金额 */
    total_amount = quantity * unit_price;
    discount_amount = total_amount * discount;
    discounted_amount = total_amount - discount_amount;
    
    /* 计算会员折扣加成 */
    select(membership_level);
        when('Platinum') member_discount_bonus = 0.02;
        when('Gold') member_discount_bonus = 0.01;
        when('Silver') member_discount_bonus = 0.005;
        otherwise member_discount_bonus = 0;
    end;
    
    /* 计算最终折扣金额 */
    final_discount = discount + member_discount_bonus;
    if final_discount > 0.2 then final_discount = 0.2;
    final_amount = total_amount * (1 - final_discount);
    
    /* 计算利润（假设毛利率为30%） */
    gross_margin = 0.3;
    estimated_profit = final_amount * gross_margin;
    
    /* 订单状态标记 */
    if discounted_amount > 0 then has_discount = '是';
    else has_discount = '否';
    
    /* 订单大小分类 */
    if quantity = 1 then order_size = '单件';
    else if quantity <= 5 then order_size = '小批量';
    else if quantity <= 10 then order_size = '中批量';
    else order_size = '大批量';
    
    /* 格式化新变量 */
    format 
        total_amount dollar12.2
        discount_amount dollar12.2
        discounted_amount dollar12.2
        member_discount_bonus percent8.2
        final_discount percent8.2
        final_amount dollar12.2
        estimated_profit dollar12.2;
    
    label 
        total_amount = '总金额'
        discount_amount = '折扣金额'
        discounted_amount = '折扣后金额'
        member_discount_bonus = '会员折扣加成'
        final_discount = '最终折扣率'
        final_amount = '最终金额'
        estimated_profit = '预估利润'
        has_discount = '是否有折扣'
        order_size = '订单大小';
run;

%put 数据清洗完成，观测数: &sysnobs;
%export_to_csv(dataset=work.clean_sales);

/* ================================================
   第五部分：数据排序 (Proc Sort)
   ================================================ */

%put 开始数据排序...;

/* 按地区和订单日期排序 */
proc sort data=work.clean_sales out=work.sales_by_region;
    by region descending order_date;
run;

/* 按产品和数量排序 */
proc sort data=work.clean_sales out=work.sales_by_product;
    by product descending quantity;
run;

/* 按会员等级和最终金额排序 */
proc sort data=work.clean_sales out=work.sales_by_membership;
    by membership_level descending final_amount;
run;

%put 数据排序完成;
%export_to_csv(dataset=work.sales_by_region);
%export_to_csv(dataset=work.sales_by_product);
%export_to_csv(dataset=work.sales_by_membership);

/* ================================================
   第六部分：数据分析 (Proc SQL)
   ================================================ */

%put 开始数据分析...;

/* 6.1 按地区汇总 */
proc sql;
    create table work.region_summary as
    select 
        region,
        count(distinct order_id) as total_orders,
        count(distinct customer_id) as unique_customers,
        sum(quantity) as total_quantity,
        sum(total_amount) as total_revenue format=dollar12.2,
        sum(discount_amount) as total_discount format=dollar12.2,
        sum(final_amount) as total_final_revenue format=dollar12.2,
        sum(estimated_profit) as total_profit format=dollar12.2,
        avg(final_amount) as avg_order_value format=dollar10.2,
        avg(discount) as avg_discount_rate format=percent8.2
    from work.clean_sales
    group by region
    order by total_final_revenue desc;
quit;

/* 6.2 按产品汇总 */
proc sql;
    create table work.product_summary as
    select 
        product,
        count(distinct order_id) as total_orders,
        sum(quantity) as total_quantity_sold,
        sum(total_amount) as total_revenue format=dollar12.2,
        sum(final_amount) as total_final_revenue format=dollar12.2,
        avg(unit_price) as avg_unit_price format=dollar10.2,
        avg(discount) as avg_discount format=percent8.2
    from work.clean_sales
    group by product
    order by total_final_revenue desc;
quit;

/* 6.3 按会员等级汇总 */
proc sql;
    create table work.membership_summary as
    select 
        membership_level,
        count(distinct order_id) as total_orders,
        count(distinct customer_id) as customer_count,
        sum(final_amount) as total_spent format=dollar12.2,
        avg(final_amount) as avg_spent_per_order format=dollar10.2,
        avg(member_discount_bonus) as avg_member_discount format=percent8.2
    from work.clean_sales
    group by membership_level
    order by total_spent desc;
quit;

/* 6.4 按年龄分组汇总 */
proc sql;
    create table work.age_group_summary as
    select 
        age_group,
        count(distinct order_id) as total_orders,
        count(distinct customer_id) as customer_count,
        sum(final_amount) as total_spent format=dollar12.2,
        avg(age) as avg_age format=8.1
    from work.clean_sales
    group by age_group
    order by total_spent desc;
quit;

/* 6.5 按订单大小汇总 */
proc sql;
    create table work.order_size_summary as
    select 
        order_size,
        count(distinct order_id) as order_count,
        sum(quantity) as total_units,
        sum(final_amount) as total_revenue format=dollar12.2,
        avg(final_amount) as avg_revenue_per_order format=dollar10.2
    from work.clean_sales
    group by order_size
    order by total_revenue desc;
quit;

/* 6.6 计算总体统计 - 修复：使用Data Step创建带常量的数据集 */
data work.overall_stats;
    set work.clean_sales end=last;
    
    retain total_orders total_customers total_units_sold 
           total_revenue total_discount_given total_final_revenue 
           total_estimated_profit sum_order_value sum_discount 0;
    retain first_order_date '31DEC9999'd last_order_date '01JAN1960'd;
    
    total_orders + 1;
    if not missing(customer_id) then do;
        /* 简单计数，实际应该用distinct，但这里简化处理 */
    end;
    total_units_sold + quantity;
    total_revenue + total_amount;
    total_discount_given + discount_amount;
    total_final_revenue + final_amount;
    total_estimated_profit + estimated_profit;
    sum_order_value + final_amount;
    sum_discount + discount;
    
    if order_date < first_order_date then first_order_date = order_date;
    if order_date > last_order_date then last_order_date = order_date;
    
    if last then do;
        length category $20;
        category = '总体';
        avg_order_value = sum_order_value / total_orders;
        avg_discount_rate = sum_discount / total_orders;
        
        format 
            total_revenue total_discount_given total_final_revenue 
            total_estimated_profit avg_order_value dollar12.2
            avg_discount_rate percent8.2
            first_order_date last_order_date yymmdd10.;
        
        output;
    end;
    
    keep category total_orders total_units_sold total_revenue 
         total_discount_given total_final_revenue total_estimated_profit
         avg_order_value avg_discount_rate first_order_date last_order_date;
run;

/* 或者使用更简单的方法：先Proc SQL汇总，再添加category */
proc sql;
    create table work.overall_stats_temp as
    select 
        count(distinct order_id) as total_orders,
        count(distinct customer_id) as total_customers,
        sum(quantity) as total_units_sold,
        sum(total_amount) as total_revenue format=dollar12.2,
        sum(discount_amount) as total_discount_given format=dollar12.2,
        sum(final_amount) as total_final_revenue format=dollar12.2,
        sum(estimated_profit) as total_estimated_profit format=dollar12.2,
        avg(final_amount) as avg_order_value format=dollar10.2,
        avg(discount) as avg_discount_rate format=percent8.2,
        min(order_date) as first_order_date format=yymmdd10.,
        max(order_date) as last_order_date format=yymmdd10.
    from work.clean_sales;
quit;

data work.overall_stats;
    length category $20;
    set work.overall_stats_temp;
    category = '总体';
run;

%put 数据分析完成;
%export_to_csv(dataset=work.region_summary);
%export_to_csv(dataset=work.product_summary);
%export_to_csv(dataset=work.membership_summary);
%export_to_csv(dataset=work.age_group_summary);
%export_to_csv(dataset=work.order_size_summary);
%export_to_csv(dataset=work.overall_stats);

/* ================================================
   第七部分：使用宏程序进行额外分析
   ================================================ */

%put 调用宏程序进行额外分析...;

/* 按地区计算最终金额统计 */
%calculate_stats(dataset=work.clean_sales, var=final_amount, group_var=region);
%export_to_csv(dataset=work.clean_sales_stats);

/* 按产品计算数量统计 */
%calculate_stats(dataset=work.clean_sales, var=quantity, group_var=product);
%export_to_csv(dataset=work.clean_sales_stats);

/* ================================================
   第八部分：创建最终输出数据集
   ================================================ */

%put 创建最终输出数据集...;

/* 8.1 创建详细订单数据 */
data work.detailed_orders;
    set work.clean_sales;
    keep 
        order_id order_date region product quantity unit_price
        discount total_amount discount_amount discounted_amount
        final_discount final_amount estimated_profit
        customer_id customer_name age gender membership_level
        age_group order_size value_category has_discount;
run;
%export_to_csv(dataset=work.detailed_orders);

/* 8.2 重新设计汇总数据输出 - 每个汇总表单独处理，确保变量一致 */

/* 总体统计 */
data work.overall_for_export;
    length summary_type $30 category $50;
    set work.overall_stats;
    summary_type = '总体统计';
    category = '总体';
    metric1 = total_orders;
    metric2 = total_revenue;
    metric3 = avg_order_value;
    label 
        summary_type = '汇总类型'
        category = '分类'
        metric1 = '订单数'
        metric2 = '总金额'
        metric3 = '平均订单金额';
    keep summary_type category metric1 metric2 metric3;
run;
%export_to_csv(dataset=work.overall_for_export);

/* 地区汇总 */
data work.region_for_export;
    length summary_type $30 category $50;
    set work.region_summary;
    summary_type = '地区汇总';
    category = region;
    metric1 = total_orders;
    metric2 = total_final_revenue;
    metric3 = avg_order_value;
    keep summary_type category metric1 metric2 metric3;
run;
%export_to_csv(dataset=work.region_for_export);

/* 产品汇总 */
data work.product_for_export;
    length summary_type $30 category $50;
    set work.product_summary;
    summary_type = '产品汇总';
    category = product;
    metric1 = total_orders;
    metric2 = total_final_revenue;
    metric3 = avg_unit_price;
    keep summary_type category metric1 metric2 metric3;
run;
%export_to_csv(dataset=work.product_for_export);

/* 会员等级汇总 */
data work.membership_for_export;
    length summary_type $30 category $50;
    set work.membership_summary;
    summary_type = '会员等级汇总';
    category = membership_level;
    metric1 = total_orders;
    metric2 = total_spent;
    metric3 = avg_spent_per_order;
    keep summary_type category metric1 metric2 metric3;
run;
%export_to_csv(dataset=work.membership_for_export);

/* 年龄分组汇总 */
data work.age_for_export;
    length summary_type $30 category $50;
    set work.age_group_summary;
    summary_type = '年龄分组汇总';
    category = age_group;
    metric1 = total_orders;
    metric2 = total_spent;
    metric3 = avg_age;
    keep summary_type category metric1 metric2 metric3;
run;
%export_to_csv(dataset=work.age_for_export);

/* 订单大小汇总 */
data work.ordersize_for_export;
    length summary_type $30 category $50;
    set work.order_size_summary;
    summary_type = '订单大小汇总';
    category = order_size;
    metric1 = order_count;
    metric2 = total_revenue;
    metric3 = avg_revenue_per_order;
    keep summary_type category metric1 metric2 metric3;
run;
%export_to_csv(dataset=work.ordersize_for_export);

/* 合并所有汇总数据 */
data work.final_output;
    set 
        work.overall_for_export
        work.region_for_export
        work.product_for_export
        work.membership_for_export
        work.age_for_export
        work.ordersize_for_export;
    format metric2 metric3 dollar12.2;
run;
%export_to_csv(dataset=work.final_output);

/* ================================================
   第九部分：打印结果报告
   ================================================ */

%put 生成结果报告...;

title1 '销售数据分析报告';
title2 '生成时间: &sysdate9. &systime';

/* 总体统计 */
title3 '一、总体统计';
proc print data=work.overall_stats label noobs;
run;

/* 地区汇总 */
title3 '二、按地区汇总';
proc print data=work.region_summary label noobs;
run;

/* 产品汇总 */
title3 '三、按产品汇总';
proc print data=work.product_summary label noobs;
run;

/* 会员等级汇总 */
title3 '四、按会员等级汇总';
proc print data=work.membership_summary label noobs;
run;

/* 年龄分组汇总 */
title3 '五、按年龄分组汇总';
proc print data=work.age_group_summary label noobs;
run;

/* 订单大小汇总 */
title3 '六、按订单大小汇总';
proc print data=work.order_size_summary label noobs;
run;

/* 详细订单数据前10条 */
title3 '七、详细订单数据（前10条）';
proc print data=work.detailed_orders(obs=10) label noobs;
run;

/* 最终输出数据 */
title3 '八、汇总输出数据';
proc print data=work.final_output label noobs;
run;

title;

/* ================================================
   第十部分：导出结果到CSV (Proc Export)
   ================================================ */

%put 开始导出数据到CSV...;

/* 导出详细订单数据 */
proc export data=work.detailed_orders
    outfile="&output_file"
    dbms=csv
    replace;
    delimiter=',';
    putnames=yes;
run;

/* 导出汇总数据 */
proc export data=work.final_output
    outfile="&output_path.sales_summary.csv"
    dbms=csv
    replace;
    delimiter=',';
    putnames=yes;
run;

/* ================================================
   第十一部分：程序结束信息
   ================================================ */

%put ================================================;
%put 程序执行完成！;
%put 输出文件1: &output_file;
%put 输出文件2: &output_path.sales_summary.csv;
%put ================================================;
