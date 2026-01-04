create database mydb;
create schema projects;

CREATE OR REPLACE FILE FORMAT parquet_ff
TYPE = PARQUET;


CREATE OR REPLACE STORAGE INTEGRATION s3_ext_intg
TYPE = 'EXTERNAL_STAGE'
ENABLED = TRUE
STORAGE_PROVIDER = 'S3'
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::accountid:role/aws-snowflake-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://bucket_name/curated/fact_sales/');


desc storage integration s3_ext_intg;

CREATE OR REPLACE STAGE s3_stage
URL = 's3://bucket_name/curated/fact_sales/'
STORAGE_INTEGRATION = s3_ext_intg
FILE_FORMAT = (TYPE=PARQUET);