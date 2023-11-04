LOAD DATA local INFILE '/tmp/data/olist_customers_dataset.csv'
INTO TABLE customers FIELDS TERMINATED BY ',' enclosed by '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;