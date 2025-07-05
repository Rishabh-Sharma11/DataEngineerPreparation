-- CSV SerDe
CREATE TABLE orders_csv (
    OrderID STRING,
    CustomerDetails STRING,
    Product STRING,
    Status STRING,
    Price DECIMAL(10, 2)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = '"',
    "escapeChar" = "\\"
)
STORED AS TEXTFILE
tblproperties ("skip.header.line.count" = "1");

load data local inpath 'file:///home/shashankde219/orders_data.csv' into table orders;

====================================================================

-- Command to create a new table from different table
create table orders_csv_backup as select * from orders_csv;

====================================================================

-- JSON Serde
CREATE TABLE orders_json (
    order_id STRING,
    customer STRUCT<
        name:STRING,
        email:STRING,
        address:STRUCT<city:STRING, state:STRING, zip:STRING>
    >,
    products ARRAY<STRUCT<product_id:STRING, name:STRING, price:DOUBLE>>,
    order_total DOUBLE,
    status STRING,
    tags ARRAY<STRING>
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE;

load data local inpath 'file:///home/shashankde219/orders_data.json' into table orders_json;

====================================================================

-- create csv table for sales data
create table automobile_sales_data ( 
    ORDERNUMBER int, 
    QUANTITYORDERED int, 
    PRICEEACH float, 
    ORDERLINENUMBER int, 
    SALES float, 
    STATUS string, 
    QTR_ID int, 
    MONTH_ID int, 
    YEAR_ID int, 
    PRODUCTLINE string, 
    MSRP int, 
    PRODUCTCODE string, 
    PHONE string, 
    CITY string, 
    STATE string, 
    POSTALCODE string, 
    COUNTRY string, 
    TERRITORY string, 
    CONTACTLASTNAME string, 
    CONTACTFIRSTNAME string, 
    DEALSIZE string 
) 
row format delimited 
fields terminated by ',' 
tblproperties("skip.header.line.count"="1") ;

-- load sales_order_data.csv data into above mentioned tables
load data local inpath 'file:///home/growdataskills/sales_order_data.csv' into table sales_order_data_csv_v1;

-- create orc file format table
create table automobile_sales_data_orc 
( 
    ORDERNUMBER int, 
    QUANTITYORDERED int, 
    PRICEEACH float, 
    ORDERLINENUMBER int, 
    SALES float, 
    STATUS string, 
    QTR_ID int, 
    MONTH_ID int, 
    YEAR_ID int, 
    PRODUCTLINE string, 
    MSRP int, 
    PRODUCTCODE string, 
    PHONE string, 
    CITY string, 
    STATE string, 
    POSTALCODE string, 
    COUNTRY string, 
    TERRITORY string, 
    CONTACTLASTNAME string, 
    CONTACTFIRSTNAME string, 
    DEALSIZE string 
) 
stored as orc;

insert into automobile_sales_data_orc select * automobile_sales_data;

-- create parquet file format table
create table automobile_sales_data_parquet 
( 
    ORDERNUMBER int, 
    QUANTITYORDERED int, 
    PRICEEACH float, 
    ORDERLINENUMBER int, 
    SALES float, 
    STATUS string, 
    QTR_ID int, 
    MONTH_ID int, 
    YEAR_ID int, 
    PRODUCTLINE string, 
    MSRP int, 
    PRODUCTCODE string, 
    PHONE string, 
    CITY string, 
    STATE string, 
    POSTALCODE string, 
    COUNTRY string, 
    TERRITORY string, 
    CONTACTLASTNAME string, 
    CONTACTFIRSTNAME string, 
    DEALSIZE string 
) 
stored as parquet;

insert overwrite table automobile_sales_data_parquet select * automobile_sales_data;

-- Hive group by query
select year_id, sum(sales) as total_sales from automobile_sales_data_orc group by year_id;

====================================================================

-- Set this property if doing static partition
set hive.mapred.mode=strict;

-- create table command for partition tables - for Static
create table automobile_sales_data_static_part
(
ORDERNUMBER int,
QUANTITYORDERED int,
SALES float,
YEAR_ID int
)
partitioned by (COUNTRY string);

-- load data in static partition
insert overwrite table automobile_sales_data_static_part partition(country = 'USA') select ordernumber,quantityordered,sales,year_id from automobile_sales_data_orc where country = 'USA';

====================================================================

-- set this property for dynamic partioning
set hive.exec.dynamic.partition.mode=nonstrict;

create table automobile_sales_data_dynamic_part
( ORDERNUMBER int,
QUANTITYORDERED int,
SALES float,
YEAR_ID int
) partitioned by (COUNTRY string);

-- load data in dynamic partition table
insert overwrite table automobile_sales_data_dynamic_part partition(country) select ordernumber,quantityordered,sales,year_id,country from automobile_sales_data_orc ;

====================================================================

-- multilevel partition
create table automobile_sales_dynamic_multilevel_part
( 
    ORDERNUMBER int,
    QUANTITYORDERED int,
    SALES float
) 
partitioned by (COUNTRY string, YEAR_ID int);

-- load data in multilevel partitions
insert overwrite table automobile_sales_dynamic_multilevel_part partition(country,year_id) select ordernumber,quantityordered,sales,country ,year_id from automobile_sales_data_orc;

====================================================================

CREATE TABLE IF NOT EXISTS cc_records (
    card_type_code STRING,
    card_type_full_name STRING,
    issuing_bank STRING,
    card_number STRING,
    card_holder_name STRING,
    cvv_cvv2 STRING,
    issue_date STRING,
    expiry_date STRING,
    billing_date INT,
    card_pin INT,
    credit_limit INT,
    dummy_column STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

load data local inpath 'file:///home/datawalebhaiya219/credit_card_records.csv' into table cc_records;

CREATE TABLE IF NOT EXISTS credit_card_types (
    card_type_code STRING,
    description STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

load data local inpath 'file:///home/datawalebhaiya219/credit_card_types_with_description.csv' into table credit_card_types;

===============================================================

-- Set Properties and Command to create buckets
set hive.enforce.bucketing=true;

CREATE TABLE IF NOT EXISTS cc_records_bucketed (
    card_type_code STRING,
    card_type_full_name STRING,
    issuing_bank STRING,
    card_number STRING,
    card_holder_name STRING,
    cvv_cvv2 STRING,
    issue_date STRING,
    expiry_date STRING,
    billing_date INT,
    card_pin INT,
    credit_limit INT,
    dummy_column STRING
)
clustered by (card_number)
sorted by (issue_date)
into 4 buckets;

insert overwrite table cc_records_bucketed select * from cc_records;

CREATE TABLE IF NOT EXISTS credit_card_types_bucketed (
    card_type_code STRING,
    description STRING
)
clustered by (card_type_code)
sorted by (card_type_code)
into 2 buckets;

insert overwrite table credit_card_types_bucketed select * from credit_card_types;


====================================================================


-- Map Side Join
SET hive.auto.convert.join=true; 

SELECT * FROM cc_records ctx LEFT JOIN credit_card_types cctype ON ctx.card_type_code = cctype.card_type_code limit 10;

====================================================================

CREATE TABLE IF NOT EXISTS cc_records_bucketed_new (
    card_type_code STRING,
    card_type_full_name STRING,
    issuing_bank STRING,
    card_number STRING,
    card_holder_name STRING,
    cvv_cvv2 STRING,
    issue_date STRING,
    expiry_date STRING,
    billing_date INT,
    card_pin INT,
    credit_limit INT,
    dummy_column STRING
)
clustered by (card_type_code)
sorted by (card_type_code)
into 2 buckets;


-- Bucket Map Join
set hive.optimize.bucketmapjoin=true; 
SET hive.auto.convert.join=true;

SELECT * FROM cc_records_bucketed_new ctx LEFT JOIN credit_card_types_bucketed cctype ON ctx.card_type_code = cctype.card_type_code limit 10;


====================================================================


-- Sorted Merge Bucket Map Join
set hive.auto.convert.sortmerge.join=true; 
set hive.optimize.bucketmapjoin.sortedmerge = true;

SELECT * FROM cc_records_bucketed_new ctx LEFT JOIN credit_card_types_bucketed cctype ON ctx.card_type_code = cctype.card_type_code limit 10;