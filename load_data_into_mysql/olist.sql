CREATE DATABASE olist;
USE olist;

CREATE TABLE customers (
    customer_id varchar(64) ,
    customer_unique_id varchar(32) ,
    customer_zip_code_prefix INT,
    customer_city varchar(64) ,
    customer_state varchar(64) ,
	PRIMARY KEY (customer_id)
);