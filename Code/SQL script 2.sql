-- CAST(dateadd(S, CAST(LEFT(tpep_Pickup_DateTime,10) AS BIGINT), '1970-01-01') AS DATE) 



SELECT top 10
    CAST(dateadd(S, CAST(LEFT(tpep_Pickup_DateTime,10) AS BIGINT), '1970-01-01') AS DATE) AS [current_day]
  , *
FROM
    OPENROWSET(
        BULK 'abfss://tkdlsgfilesys@tkdlsg.dfs.core.windows.net/data/taxi/raw/2022/*/*.parquet',
        FORMAT='PARQUET'
    ) AS [nyc]
WHERE nyc.filepath(1) = '6'



SELECT
    CAST(dateadd(S, CAST(LEFT(tpep_Pickup_DateTime,10) AS BIGINT), '1970-01-01') AS DATE) AS [current_day]
    ,count(vendorID) as rides_per_day
FROM
    OPENROWSET(
        BULK 'abfss://tkdlsgfilesys@tkdlsg.dfs.core.windows.net/data/taxi/raw/2022/*/*.parquet',
        FORMAT='PARQUET'
    ) AS [nyc]
WHERE nyc.filepath(1) = '6'
GROUP BY CAST(dateadd(S, CAST(LEFT(tpep_Pickup_DateTime,10) AS BIGINT), '1970-01-01') AS DATE) 
ORDER BY 1 ASC




SELECT 
    CAST(dateadd(S, CAST(LEFT(tpep_Pickup_DateTime,10) AS BIGINT), '1970-01-01') AS DATE) AS [current_day]
    ,cast(count(*)  as bigint) as nof
   -- ,count(*) AS Rides_per_day
FROM
    OPENROWSET( BULK 'abfss://tkdlsgfilesys@tkdlsg.dfs.core.windows.net/data/taxi/raw/2022/*/*.parquet', FORMAT='PARQUET'  ) AS [nyc]
WHERE nyc.filepath(1) = '6'
GROUP BY     CAST(dateadd(S, CAST(LEFT(tpep_Pickup_DateTime,10) AS BIGINT), '1970-01-01') AS DATE) 


