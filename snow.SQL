CREATE STORAGE INTEGRATION stock_hist
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<XXX>:role/snowflake'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<XXX>/')
;

create or replace stage hist_parquet
storage_integration = stock_hist
url = 's3://<XXX>/'
;

select system$get_snowflake_platform_info();
;

create or replace file format parq_hist_format
  type = parquet
  compression = snappy
;

select        $1
     ,metadata$filename as metadata_filename
     ,metadata$file_row_number as metadata_file_row_number
     ,current_timestamp() as load_timestamp from @hist_parquet (file_format => parq_hist_format) t 
;

     
create or replace external table stock_price_history_ext
  with location = @hist_parquet/test/data/
  auto_refresh = true
  file_format = (type = parquet)
  pattern='.*.*[.]parquet'
;

create or replace table stock_price_history
AS
select 'AA' AS Name, VALUE:"Date"::DateTime AS DATE,
TO_VARCHAR(VALUE:Price) AS Price from stock_price_history_ext
;

select * from stock_price_history
  match_recognize(
    partition by name
    order by date
    measures
      match_number() as match_number,
      first(date) as start_v,
      last(date) as end_v,
      count(*) as rows_in_sequence,
      count(row_with_price_decrease.*) as num_decreases,
      count(row_with_price_increase.*) as num_increases
    one row per match
    after match skip to last row_with_price_increase
    pattern(V row_with_price_decrease+ row_with_price_increase+)
    define
      row_with_price_decrease as price < lag(price),
      row_with_price_increase as price > lag(price)
  )
order by name, start_v
;

SELECT *
FROM stock_price_history MATCH_RECOGNIZE (
PARTITION BY name ORDER BY date
measures 
         match_number() as match_number,
         first(date) AS start_w,
         last(date) AS end_w,
         count(*) as rows_in_sequence
ONE ROW PER MATCH
after match skip to last z
PATTERN (W x+ y+ w+ z+)
DEFINE
 x AS price <= lag(price),
 y AS price >= lag(price),
 w AS price <= lag(price),
 z AS price >= lag(price)
)
ORDER BY name, start_w;