
-- Use Built-in Pool + Connect to database!

SELECT TOP 10 *  FROM OPENROWSET(BULK 'abfss://tkdlsgfilesys@tkdlsg.dfs.core.windows.net/data/taxi/raw/2022/1/*.parquet', FORMAT='PARQUET') AS [file]


-- January 2022
SELECT count(*) FROM OPENROWSET(BULK 'abfss://tkdlsgfilesys@tkdlsg.dfs.core.windows.net/data/taxi/raw/2022/1/*.parquet', FORMAT='PARQUET') AS [file]
-- rows: 2.463.931

-- February 2022
SELECT count(*) FROM OPENROWSET(BULK 'abfss://tkdlsgfilesys@tkdlsg.dfs.core.windows.net/data/taxi/raw/2022/2/*.parquet', FORMAT='PARQUET') AS [file]
-- rows: 2.979.431

-- March 2022
SELECT count(*) FROM OPENROWSET(BULK 'abfss://tkdlsgfilesys@tkdlsg.dfs.core.windows.net/data/taxi/raw/2022/3/*.parquet', FORMAT='PARQUET') AS [file]
-- rows: 3.627.882


-- All Files 2022
SELECT count(*)  FROM OPENROWSET(BULK 'abfss://tkdlsgfilesys@tkdlsg.dfs.core.windows.net/data/taxi/raw/2022/*/*.parquet', FORMAT='PARQUET') AS [file]
--Rows: 19.817.583


-- All rides from January to March
SELECT
    COUNT(*) AS rides_for_first_three_months
FROM
    OPENROWSET(
        BULK 'abfss://tkdlsgfilesys@tkdlsg.dfs.core.windows.net/data/taxi/raw/2022/*/*.parquet',
        FORMAT='PARQUET'
    ) AS [nyc]
WHERE nyc.filepath(1) >= '1' AND nyc.filepath(1) <= '3'
-- Rows: 9.071.244



-- Looking into data
SELECT
    YEAR(dateadd(S, CAST(LEFT(tpep_Pickup_DateTime,10) AS BIGINT), '1970-01-01')) AS current_year,
    COUNT(*) AS rides_for_years
FROM
    OPENROWSET(
        BULK 'abfss://tkdlsgfilesys@tkdlsg.dfs.core.windows.net/data/taxi/raw/2022/*/*.parquet',
        FORMAT='PARQUET'
    ) AS [nyc]
WHERE nyc.filepath(1) >= '1' AND nyc.filepath(1) <= '3'
GROUP BY YEAR(dateadd(S, CAST(LEFT(tpep_Pickup_DateTime,10) AS BIGINT), '1970-01-01'))
ORDER BY 1 ASC


-- Looking into data per Days in June 2022
SELECT
    dateadd(S, CAST(LEFT(tpep_Pickup_DateTime,10) AS BIGINT), '1970-01-01') AS [current_day],
    COUNT(*) as rides_per_day
FROM
    OPENROWSET(
        BULK 'abfss://tkdlsgfilesys@tkdlsg.dfs.core.windows.net/data/taxi/raw/2022/*/*.parquet',
        FORMAT='PARQUET'
    ) AS [nyc]
WHERE nyc.filepath(1) = '6'
GROUP BY dateadd(S, CAST(LEFT(tpep_Pickup_DateTime,10) AS BIGINT), '1970-01-01') 




---- Merging more datasets
WITH taxi_rides AS (
SELECT
    CAST(dateadd(S, CAST(LEFT(tpep_Pickup_DateTime,10) AS BIGINT), '1970-01-01') AS DATE) AS [current_day]
    ,COUNT(*) as rides_per_day
FROM
    OPENROWSET(
        BULK 'abfss://tkdlsgfilesys@tkdlsg.dfs.core.windows.net/data/taxi/raw/2022/*/*.parquet',
        FORMAT='PARQUET'
    ) AS [nyc]
WHERE nyc.filepath(1) = '6'
GROUP BY   CAST(dateadd(S, CAST(LEFT(tpep_Pickup_DateTime,10) AS BIGINT), '1970-01-01') AS DATE)
),
public_holidays AS (
SELECT
    holidayname as holiday,
    date
FROM
    OPENROWSET(
        BULK 'abfss://tkdlsgfilesys@tkdlsg.dfs.core.windows.net/data/holidays/*.parquet',
        FORMAT='PARQUET'
    ) AS [holidays]
WHERE countryorregion = 'Slovenia' AND YEAR(date) = 2022
),
joined_data AS (
SELECT
    *
FROM taxi_rides t
LEFT OUTER JOIN public_holidays p on t.current_day = p.date
)

SELECT 
    *,
    holiday_rides = 
    CASE   
      WHEN holiday is null THEN 0   
      WHEN holiday is not null THEN rides_per_day
    END   
FROM joined_data
ORDER BY current_day ASC

