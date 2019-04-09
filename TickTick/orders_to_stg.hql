set hive.execution.engine=tez;

create database if not exists <<db_name>>;

drop table if exists stg_qa_sales_orders;

create table stg_qa_sales_orders as
    select order_id, dt
    from sales_orders 
    where to_date(dt) between date_sub(current_date,8) and date_sub(current_date,2) 
    and country='<<country_code>>';

drop table if exists svcwwns.stg_qa_return_orders;

create table stg_qa_return_orders as
    select order_id, dt
    from return_orders
    where to_date(dt) between date_sub(current_date,8) and date_sub(current_date,2) 
    and country='<<country_code>>';
