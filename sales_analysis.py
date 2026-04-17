#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
销售数据分析程序 - Python版本
将SAS程序转换为Python实现

功能：
1. 从CSV文件读取销售数据
2. 数据清洗和转换
3. 数据汇总分析
4. 数据排序
5. 导出结果到CSV

输入文件：sales_data.csv
输出文件：sales_summary.csv, sales_analysis_result.csv
所有中间过程和结果都输出到 python_result 目录
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime

OUTPUT_DIR = 'python_result'
LOG_DIR = os.path.join(OUTPUT_DIR, 'log')


def ensure_dir(directory):
    """确保目录存在"""
    if not os.path.exists(directory):
        os.makedirs(directory)


def round2(value):
    """四舍五入到2位小数"""
    if pd.isna(value):
        return value
    return round(value, 2)


def format_currency(value):
    """将数值格式化为货币格式（带$和千分位，四舍五入到2位）"""
    if pd.isna(value):
        return value
    rounded = round2(value)
    return f"${rounded:,.2f}"


def format_percent(value):
    """将数值格式化为百分比格式"""
    if pd.isna(value):
        return value
    return f"{value * 100:.2f}%"


def parse_currency(value):
    """解析货币格式的字符串为数值"""
    if isinstance(value, str):
        value = value.replace('$', '').replace(',', '')
    return float(value)


def export_to_csv(df, filename, log_dir=LOG_DIR):
    """
    将DataFrame导出为CSV文件到log目录
    模拟SAS的export_to_csv宏
    """
    ensure_dir(log_dir)
    
    filepath = os.path.join(log_dir, filename)
    df.to_csv(filepath, index=False, encoding='utf-8-sig')
    print(f"数据集已导出到: {filepath}")


def calculate_stats(df, var, group_var, output_name):
    """
    计算指定变量的统计量
    模拟SAS的calculate_stats宏
    """
    stats = df.groupby(group_var).agg(
        total_orders=(group_var, 'count'),
        total_var=(var, 'sum'),
        avg_var=(var, 'mean'),
        min_var=(var, 'min'),
        max_var=(var, 'max'),
        std_var=(var, 'std')
    ).reset_index()
    
    stats = stats.rename(columns={
        'total_var': f'total_{var}',
        'avg_var': f'avg_{var}',
        'min_var': f'min_{var}',
        'max_var': f'max_{var}',
        'std_var': f'std_{var}'
    })
    
    stats[f'avg_{var}'] = stats[f'avg_{var}'].apply(round2)
    stats[f'std_{var}'] = stats[f'std_{var}'].apply(round2)
    
    stats = stats.sort_values(by=f'total_{var}', ascending=False)
    
    stats_export = stats.copy()
    stats_export[f'avg_{var}'] = stats_export[f'avg_{var}'].apply(lambda x: f"{x:.2f}")
    stats_export[f'std_{var}'] = stats_export[f'std_{var}'].apply(lambda x: f"{x:.2f}")
    
    export_to_csv(stats_export, f'{output_name}.csv')
    return stats


def main():
    ensure_dir(OUTPUT_DIR)
    ensure_dir(LOG_DIR)
    
    print("=" * 50)
    print("程序开始执行")
    print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    
    input_file = 'sales_data.csv'
    output_file = os.path.join(OUTPUT_DIR, 'sales_analysis_result.csv')
    summary_file = os.path.join(OUTPUT_DIR, 'sales_summary.csv')
    
    print(f"输入文件: {input_file}")
    print(f"输出文件1: {output_file}")
    print(f"输出文件2: {summary_file}")
    
    # ================================================
    # 第三部分：读取CSV数据 (Data Step) - raw_sales
    # ================================================
    print("\n" + "=" * 50)
    print("开始读取CSV数据...")
    print("=" * 50)
    
    raw_sales = pd.read_csv(input_file, parse_dates=['order_date'])
    
    print(f"原始数据读取完成，观测数: {len(raw_sales)}")
    
    raw_sales['order_date'] = pd.to_datetime(raw_sales['order_date'])
    
    raw_sales = raw_sales[raw_sales['order_id'].notna()].copy()
    
    raw_sales.loc[raw_sales['quantity'] < 0, 'quantity'] = 0
    raw_sales.loc[raw_sales['unit_price'] < 0, 'unit_price'] = 0
    raw_sales.loc[raw_sales['discount'] < 0, 'discount'] = 0
    raw_sales.loc[raw_sales['discount'] > 1, 'discount'] = 1
    
    raw_sales['order_month'] = raw_sales['order_date'].dt.strftime('%Y-%m')
    
    def get_age_group(age):
        if age < 30:
            return '18-29'
        elif age < 40:
            return '30-39'
        elif age < 50:
            return '40-49'
        else:
            return '50+'
    
    raw_sales['age_group'] = raw_sales['age'].apply(get_age_group)
    
    raw_sales['order_value'] = raw_sales['quantity'] * raw_sales['unit_price']
    
    def get_value_category(value):
        if value < 500:
            return '低价值'
        elif value < 2000:
            return '中价值'
        else:
            return '高价值'
    
    raw_sales['value_category'] = raw_sales['order_value'].apply(get_value_category)
    
    raw_sales_export = raw_sales.copy()
    raw_sales_export['order_date'] = raw_sales_export['order_date'].dt.strftime('%Y-%m-%d')
    raw_sales_export['order_value'] = raw_sales_export['order_value'].apply(round2)
    raw_sales_export['unit_price'] = raw_sales_export['unit_price'].apply(format_currency)
    raw_sales_export['discount'] = raw_sales_export['discount'].apply(format_percent)
    
    export_to_csv(raw_sales_export, 'raw_sales.csv')
    
    # ================================================
    # 第四部分：数据清洗和转换 (Data Step) - clean_sales
    # ================================================
    print("\n" + "=" * 50)
    print("开始数据清洗和转换...")
    print("=" * 50)
    
    clean_sales = raw_sales.copy()
    
    clean_sales['total_amount'] = clean_sales['quantity'] * clean_sales['unit_price']
    clean_sales['discount_amount'] = clean_sales['total_amount'] * clean_sales['discount']
    clean_sales['discounted_amount'] = clean_sales['total_amount'] - clean_sales['discount_amount']
    
    def get_member_discount_bonus(level):
        if level == 'Platinum':
            return 0.02
        elif level == 'Gold':
            return 0.01
        elif level == 'Silver':
            return 0.005
        else:
            return 0
    
    clean_sales['member_discount_bonus'] = clean_sales['membership_level'].apply(get_member_discount_bonus)
    
    clean_sales['final_discount'] = clean_sales['discount'] + clean_sales['member_discount_bonus']
    clean_sales.loc[clean_sales['final_discount'] > 0.2, 'final_discount'] = 0.2
    clean_sales['final_amount'] = clean_sales['total_amount'] * (1 - clean_sales['final_discount'])
    
    clean_sales['gross_margin'] = 0.3
    clean_sales['estimated_profit'] = clean_sales['final_amount'] * clean_sales['gross_margin']
    
    clean_sales['has_discount'] = np.where(clean_sales['discounted_amount'] > 0, '是', '否')
    
    def get_order_size(quantity):
        if quantity == 1:
            return '单件'
        elif quantity <= 5:
            return '小批'
        elif quantity <= 10:
            return '中批'
        else:
            return '大批'
    
    clean_sales['order_size'] = clean_sales['quantity'].apply(get_order_size)
    
    print(f"数据清洗完成，观测数: {len(clean_sales)}")
    
    clean_sales_export = clean_sales.copy()
    clean_sales_export['order_date'] = clean_sales_export['order_date'].dt.strftime('%Y-%m-%d')
    clean_sales_export['order_value'] = clean_sales_export['order_value'].apply(round2)
    clean_sales_export['unit_price'] = clean_sales_export['unit_price'].apply(format_currency)
    clean_sales_export['discount'] = clean_sales_export['discount'].apply(format_percent)
    clean_sales_export['total_amount'] = clean_sales_export['total_amount'].apply(format_currency)
    clean_sales_export['discount_amount'] = clean_sales_export['discount_amount'].apply(format_currency)
    clean_sales_export['discounted_amount'] = clean_sales_export['discounted_amount'].apply(format_currency)
    clean_sales_export['member_discount_bonus'] = clean_sales_export['member_discount_bonus'].apply(format_percent)
    clean_sales_export['final_discount'] = clean_sales_export['final_discount'].apply(format_percent)
    clean_sales_export['final_amount'] = clean_sales_export['final_amount'].apply(format_currency)
    clean_sales_export['estimated_profit'] = clean_sales_export['estimated_profit'].apply(format_currency)
    
    export_to_csv(clean_sales_export, 'clean_sales.csv')
    
    # ================================================
    # 第五部分：数据排序 (Proc Sort)
    # ================================================
    print("\n" + "=" * 50)
    print("开始数据排序...")
    print("=" * 50)
    
    sales_by_region = clean_sales.sort_values(
        by=['region', 'order_date'],
        ascending=[True, False]
    ).copy()
    
    sales_by_product = clean_sales.sort_values(
        by=['product', 'quantity'],
        ascending=[True, False]
    ).copy()
    
    sales_by_membership = clean_sales.sort_values(
        by=['membership_level', 'final_amount'],
        ascending=[True, False]
    ).copy()
    
    print("数据排序完成")
    
    def sort_export(df, filename):
        df_export = df.copy()
        df_export['order_date'] = df_export['order_date'].dt.strftime('%Y-%m-%d')
        df_export['order_value'] = df_export['order_value'].apply(round2)
        df_export['unit_price'] = df_export['unit_price'].apply(format_currency)
        df_export['discount'] = df_export['discount'].apply(format_percent)
        df_export['total_amount'] = df_export['total_amount'].apply(format_currency)
        df_export['discount_amount'] = df_export['discount_amount'].apply(format_currency)
        df_export['discounted_amount'] = df_export['discounted_amount'].apply(format_currency)
        df_export['member_discount_bonus'] = df_export['member_discount_bonus'].apply(format_percent)
        df_export['final_discount'] = df_export['final_discount'].apply(format_percent)
        df_export['final_amount'] = df_export['final_amount'].apply(format_currency)
        df_export['estimated_profit'] = df_export['estimated_profit'].apply(format_currency)
        export_to_csv(df_export, filename)
    
    sort_export(sales_by_region, 'sales_by_region.csv')
    sort_export(sales_by_product, 'sales_by_product.csv')
    sort_export(sales_by_membership, 'sales_by_membership.csv')
    
    # ================================================
    # 第六部分：数据分析 (Proc SQL)
    # ================================================
    print("\n" + "=" * 50)
    print("开始数据分析...")
    print("=" * 50)
    
    region_summary = clean_sales.groupby('region').agg(
        total_orders=('order_id', 'nunique'),
        unique_customers=('customer_id', 'nunique'),
        total_quantity=('quantity', 'sum'),
        total_revenue=('total_amount', 'sum'),
        total_discount=('discount_amount', 'sum'),
        total_final_revenue=('final_amount', 'sum'),
        total_profit=('estimated_profit', 'sum'),
        avg_order_value=('final_amount', 'mean'),
        avg_discount_rate=('discount', 'mean')
    ).reset_index()
    
    region_summary = region_summary.sort_values(by='total_final_revenue', ascending=False)
    
    region_summary_export = region_summary.copy()
    region_summary_export['total_revenue'] = region_summary_export['total_revenue'].apply(format_currency)
    region_summary_export['total_discount'] = region_summary_export['total_discount'].apply(format_currency)
    region_summary_export['total_final_revenue'] = region_summary_export['total_final_revenue'].apply(format_currency)
    region_summary_export['total_profit'] = region_summary_export['total_profit'].apply(format_currency)
    region_summary_export['avg_order_value'] = region_summary_export['avg_order_value'].apply(format_currency)
    region_summary_export['avg_discount_rate'] = region_summary_export['avg_discount_rate'].apply(format_percent)
    
    export_to_csv(region_summary_export, 'region_summary.csv')
    
    product_summary = clean_sales.groupby('product').agg(
        total_orders=('order_id', 'nunique'),
        total_quantity_sold=('quantity', 'sum'),
        total_revenue=('total_amount', 'sum'),
        total_final_revenue=('final_amount', 'sum'),
        avg_unit_price=('unit_price', 'mean'),
        avg_discount=('discount', 'mean')
    ).reset_index()
    
    product_summary = product_summary.sort_values(by='total_final_revenue', ascending=False)
    
    product_summary_export = product_summary.copy()
    product_summary_export['total_revenue'] = product_summary_export['total_revenue'].apply(format_currency)
    product_summary_export['total_final_revenue'] = product_summary_export['total_final_revenue'].apply(format_currency)
    product_summary_export['avg_unit_price'] = product_summary_export['avg_unit_price'].apply(format_currency)
    product_summary_export['avg_discount'] = product_summary_export['avg_discount'].apply(format_percent)
    
    export_to_csv(product_summary_export, 'product_summary.csv')
    
    membership_summary = clean_sales.groupby('membership_level').agg(
        total_orders=('order_id', 'nunique'),
        customer_count=('customer_id', 'nunique'),
        total_spent=('final_amount', 'sum'),
        avg_spent_per_order=('final_amount', 'mean'),
        avg_member_discount=('member_discount_bonus', 'mean')
    ).reset_index()
    
    membership_summary = membership_summary.sort_values(by='total_spent', ascending=False)
    
    membership_summary_export = membership_summary.copy()
    membership_summary_export['total_spent'] = membership_summary_export['total_spent'].apply(format_currency)
    membership_summary_export['avg_spent_per_order'] = membership_summary_export['avg_spent_per_order'].apply(format_currency)
    membership_summary_export['avg_member_discount'] = membership_summary_export['avg_member_discount'].apply(format_percent)
    
    export_to_csv(membership_summary_export, 'membership_summary.csv')
    
    age_group_summary = clean_sales.groupby('age_group').agg(
        total_orders=('order_id', 'nunique'),
        customer_count=('customer_id', 'nunique'),
        total_spent=('final_amount', 'sum'),
        avg_age=('age', 'mean')
    ).reset_index()
    
    age_group_summary = age_group_summary.sort_values(by='total_spent', ascending=False)
    
    age_group_summary_export = age_group_summary.copy()
    age_group_summary_export['total_spent'] = age_group_summary_export['total_spent'].apply(format_currency)
    
    export_to_csv(age_group_summary_export, 'age_group_summary.csv')
    
    order_size_summary = clean_sales.groupby('order_size').agg(
        order_count=('order_id', 'nunique'),
        total_units=('quantity', 'sum'),
        total_revenue=('final_amount', 'sum'),
        avg_revenue_per_order=('final_amount', 'mean')
    ).reset_index()
    
    order_size_summary = order_size_summary.sort_values(by='total_revenue', ascending=False)
    
    order_size_summary_export = order_size_summary.copy()
    order_size_summary_export['total_revenue'] = order_size_summary_export['total_revenue'].apply(format_currency)
    order_size_summary_export['avg_revenue_per_order'] = order_size_summary_export['avg_revenue_per_order'].apply(format_currency)
    
    export_to_csv(order_size_summary_export, 'order_size_summary.csv')
    
    overall_stats = pd.DataFrame({
        'category': ['总体'],
        'total_orders': [clean_sales['order_id'].nunique()],
        'total_customers': [clean_sales['customer_id'].nunique()],
        'total_units_sold': [clean_sales['quantity'].sum()],
        'total_revenue': [clean_sales['total_amount'].sum()],
        'total_discount_given': [clean_sales['discount_amount'].sum()],
        'total_final_revenue': [clean_sales['final_amount'].sum()],
        'total_estimated_profit': [clean_sales['estimated_profit'].sum()],
        'avg_order_value': [clean_sales['final_amount'].mean()],
        'avg_discount_rate': [clean_sales['discount'].mean()],
        'first_order_date': [clean_sales['order_date'].min().strftime('%Y-%m-%d')],
        'last_order_date': [clean_sales['order_date'].max().strftime('%Y-%m-%d')]
    })
    
    overall_stats_export = overall_stats.copy()
    overall_stats_export['total_revenue'] = overall_stats_export['total_revenue'].apply(format_currency)
    overall_stats_export['total_discount_given'] = overall_stats_export['total_discount_given'].apply(format_currency)
    overall_stats_export['total_final_revenue'] = overall_stats_export['total_final_revenue'].apply(format_currency)
    overall_stats_export['total_estimated_profit'] = overall_stats_export['total_estimated_profit'].apply(format_currency)
    overall_stats_export['avg_order_value'] = overall_stats_export['avg_order_value'].apply(format_currency)
    overall_stats_export['avg_discount_rate'] = overall_stats_export['avg_discount_rate'].apply(format_percent)
    
    export_to_csv(overall_stats_export, 'overall_stats.csv')
    
    print("数据分析完成")
    
    # ================================================
    # 第七部分：使用宏程序进行额外分析
    # ================================================
    print("\n" + "=" * 50)
    print("调用宏程序进行额外分析...")
    print("=" * 50)
    
    calculate_stats(clean_sales, 'final_amount', 'region', 'clean_sales_stats')
    calculate_stats(clean_sales, 'quantity', 'product', 'clean_sales_stats')
    
    # ================================================
    # 第八部分：创建最终输出数据集
    # ================================================
    print("\n" + "=" * 50)
    print("创建最终输出数据集...")
    print("=" * 50)
    
    detailed_orders = clean_sales[[
        'order_id', 'order_date', 'region', 'product', 'quantity', 'unit_price',
        'discount', 'customer_id', 'customer_name', 'age', 'gender', 'membership_level',
        'age_group', 'value_category', 'total_amount', 'discount_amount', 'discounted_amount',
        'final_discount', 'final_amount', 'estimated_profit', 'has_discount', 'order_size'
    ]].copy()
    
    detailed_orders_export = detailed_orders.copy()
    detailed_orders_export['order_date'] = detailed_orders_export['order_date'].dt.strftime('%Y-%m-%d')
    detailed_orders_export['unit_price'] = detailed_orders_export['unit_price'].apply(format_currency)
    detailed_orders_export['discount'] = detailed_orders_export['discount'].apply(format_percent)
    detailed_orders_export['total_amount'] = detailed_orders_export['total_amount'].apply(format_currency)
    detailed_orders_export['discount_amount'] = detailed_orders_export['discount_amount'].apply(format_currency)
    detailed_orders_export['discounted_amount'] = detailed_orders_export['discounted_amount'].apply(format_currency)
    detailed_orders_export['final_discount'] = detailed_orders_export['final_discount'].apply(format_percent)
    detailed_orders_export['final_amount'] = detailed_orders_export['final_amount'].apply(format_currency)
    detailed_orders_export['estimated_profit'] = detailed_orders_export['estimated_profit'].apply(format_currency)
    
    export_to_csv(detailed_orders_export, 'detailed_orders.csv')
    
    # ================================================
    # for_export 表 - 输出原始高精度数值（不带$符号）
    # ================================================
    
    overall_for_export = pd.DataFrame({
        'summary_type': ['总体统计'],
        'category': ['总体'],
        'metric1': [overall_stats['total_orders'].iloc[0]],
        'metric2': [overall_stats['total_revenue'].iloc[0]],
        'metric3': [overall_stats['avg_order_value'].iloc[0]]
    })
    
    export_to_csv(overall_for_export, 'overall_for_export.csv')
    
    region_for_export = region_summary[['region', 'total_orders', 'total_final_revenue', 'avg_order_value']].copy()
    region_for_export['summary_type'] = '地区汇总'
    region_for_export = region_for_export.rename(columns={
        'region': 'category',
        'total_orders': 'metric1',
        'total_final_revenue': 'metric2',
        'avg_order_value': 'metric3'
    })
    region_for_export = region_for_export[['summary_type', 'category', 'metric1', 'metric2', 'metric3']]
    
    export_to_csv(region_for_export, 'region_for_export.csv')
    
    product_for_export = product_summary[['product', 'total_orders', 'total_final_revenue', 'avg_unit_price']].copy()
    product_for_export['summary_type'] = '产品汇总'
    product_for_export = product_for_export.rename(columns={
        'product': 'category',
        'total_orders': 'metric1',
        'total_final_revenue': 'metric2',
        'avg_unit_price': 'metric3'
    })
    product_for_export = product_for_export[['summary_type', 'category', 'metric1', 'metric2', 'metric3']]
    
    export_to_csv(product_for_export, 'product_for_export.csv')
    
    membership_for_export = membership_summary[['membership_level', 'total_orders', 'total_spent', 'avg_spent_per_order']].copy()
    membership_for_export['summary_type'] = '会员等级汇总'
    membership_for_export = membership_for_export.rename(columns={
        'membership_level': 'category',
        'total_orders': 'metric1',
        'total_spent': 'metric2',
        'avg_spent_per_order': 'metric3'
    })
    membership_for_export = membership_for_export[['summary_type', 'category', 'metric1', 'metric2', 'metric3']]
    
    export_to_csv(membership_for_export, 'membership_for_export.csv')
    
    age_for_export = age_group_summary[['age_group', 'total_orders', 'total_spent', 'avg_age']].copy()
    age_for_export['summary_type'] = '年龄分组汇总'
    age_for_export = age_for_export.rename(columns={
        'age_group': 'category',
        'total_orders': 'metric1',
        'total_spent': 'metric2',
        'avg_age': 'metric3'
    })
    age_for_export = age_for_export[['summary_type', 'category', 'metric1', 'metric2', 'metric3']]
    
    export_to_csv(age_for_export, 'age_for_export.csv')
    
    ordersize_for_export = order_size_summary[['order_size', 'order_count', 'total_revenue', 'avg_revenue_per_order']].copy()
    ordersize_for_export['summary_type'] = '订单大小汇总'
    ordersize_for_export = ordersize_for_export.rename(columns={
        'order_size': 'category',
        'order_count': 'metric1',
        'total_revenue': 'metric2',
        'avg_revenue_per_order': 'metric3'
    })
    ordersize_for_export = ordersize_for_export[['summary_type', 'category', 'metric1', 'metric2', 'metric3']]
    
    export_to_csv(ordersize_for_export, 'ordersize_for_export.csv')
    
    # ================================================
    # final_output - 输出格式化的货币值（带$符号，四舍五入到2位）
    # ================================================
    
    final_output = pd.concat([
        overall_for_export,
        region_for_export,
        product_for_export,
        membership_for_export,
        age_for_export,
        ordersize_for_export
    ], ignore_index=True)
    
    final_output_export = final_output.copy()
    
    final_output_export['metric2'] = final_output_export['metric2'].apply(format_currency)
    final_output_export['metric3'] = final_output_export['metric3'].apply(format_currency)
    
    export_to_csv(final_output_export, 'final_output.csv')
    
    # ================================================
    # 第十部分：导出结果到CSV
    # ================================================
    print("\n" + "=" * 50)
    print("开始导出数据到CSV...")
    print("=" * 50)
    
    detailed_orders_export.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"详细订单数据已导出到: {output_file}")
    
    final_output_export.to_csv(summary_file, index=False, encoding='utf-8-sig')
    print(f"汇总数据已导出到: {summary_file}")
    
    # ================================================
    # 第十一部分：程序结束信息
    # ================================================
    print("\n" + "=" * 50)
    print("程序执行完成！")
    print(f"输出文件1: {output_file}")
    print(f"输出文件2: {summary_file}")
    print("=" * 50)


if __name__ == '__main__':
    main()
