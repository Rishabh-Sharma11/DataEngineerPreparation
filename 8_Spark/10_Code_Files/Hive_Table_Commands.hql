CREATE DATABASE tables_by_spark;

CREATE TABLE tables_by_spark.orders (
    order_id STRING,
    customer_id STRING,
    order_status STRING,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

load data inpath '/tmp/input_data/orders_dataset.csv' into table orders;
load data local inpath 'file:///home/shashank/input_data/orders_dataset.csv' into table orders;


CREATE TABLE tables_by_spark.customers (
    customer_id STRING,
    customer_unique_id STRING,
    customer_zip_code_prefix STRING,
    customer_city STRING,
    customer_state STRING
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

load data inpath '/tmp/input_data/customers_dataset.csv' into table customers;


============================================

CREATE TABLE tables_by_spark.orders_pq (
    order_id STRING,
    customer_id STRING,
    order_status STRING,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
)
STORED AS PARQUET;

insert overwrite table tables_by_spark.orders_pq select * from tables_by_spark.orders;

CREATE TABLE tables_by_spark.customers_pq (
    customer_id STRING,
    customer_unique_id STRING,
    customer_zip_code_prefix STRING,
    customer_city STRING,
    customer_state STRING
)
STORED AS PARQUET; 

insert overwrite table tables_by_spark.customers_pq select * from tables_by_spark.customers;