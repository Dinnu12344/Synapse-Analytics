-- Query CSV file stored in ADLS Gen2 with headers using Serverless SQL Pool
SELECT *
FROM OPENROWSET(
    BULK 'https://synapsestorage2210.dfs.core.windows.net/files/data/sampledata.csv',
    FORMAT='CSV',
    PARSER_VERSION='2.0',
    HEADER_ROW = TRUE  -- This option ensures the first row is used as column headers
) AS [data]


--Query parquet files
SELECT top 100 *
FROM OPENROWSET(
    BULK 'https://synapsestorage2210.blob.core.windows.net/files/data.parquet', --'https://<storage_account>.blob.core.windows.net/<container>/data.parquet',
    FORMAT = 'PARQUET'
) AS [result]

--Querying JSON Files
SELECT *
FROM OPENROWSET(
    BULK 'https://synapsestorage2210.blob.core.windows.net/files/sampledata.json', --'https://<storage_account>.blob.core.windows.net/<container>/file.json',
    FORMAT = 'CSV',
    FIELDQUOTE = '0x0b'
) AS [result]
WITH (
    CustomerID NVARCHAR(100),
    OrderID NVARCHAR(100),
    OrderDate DATETIME
)

--Querying Delta Lake Format
SELECT *
FROM OPENROWSET(
    BULK 'https://synapsestorage2210.blob.core.windows.net/files/delta',--'https://<storage_account>.blob.core.windows.net/<container>/delta_table',
    FORMAT = 'DELTA'
) AS [result]

--Creating External Tables (CTAS)
CREATE EXTERNAL TABLE dbo.Sales
WITH (
    LOCATION = 'sales_data/',
    DATA_SOURCE = MyDataSource,
    FILE_FORMAT = MyFileFormat
)
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://synapsestorage2210.blob.core.windows.net/files/sales.csv', --'https://<storage_account>.blob.core.windows.net/<container>/sales.csv',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE
) AS [result];

--Joining Data from Multiple Files
SELECT customers.CustomerID, orders.OrderID, orders.OrderDate
FROM OPENROWSET(
    BULK 'https://synapsestorage2210.blob.core.windows.net/files/sales.csv', --'https://<storage_account>.blob.core.windows.net/<container>/customers.csv',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE
) AS customers
JOIN OPENROWSET(
    BULK 'https://synapsestorage2210.blob.core.windows.net/files/orders.csv', --'https://<storage_account>.blob.core.windows.net/<container>/orders.csv',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE
) AS orders
ON customers.CustomerID = orders.CustomerID;

--Metadata Queries
SELECT column_name, data_type
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'orders';

--Aggregating Data (GROUP BY)
SELECT ProductID, SUM(SalesAmount) AS TotalSales
FROM OPENROWSET(
    BULK 'https://synapsestorage2210.blob.core.windows.net/files/sales.parquet', --'https://<storage_account>.blob.core.windows.net/<container>/sales.parquet',
    FORMAT = 'PARQUET'
) AS [result]
GROUP BY ProductID;


--Filtering and Ordering Data
SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://synapsestorage2210.blob.core.windows.net/files/data.parquet', -- 'https://<storage_account>.blob.core.windows.net/<container>/sales.parquet',
    FORMAT = 'PARQUET'
) AS [result]
WHERE SalesAmount > 1000
ORDER BY SalesAmount DESC;

--Inserting Data into Dedicated SQL Pools
INSERT INTO dbo.SalesSummary
SELECT ProductID, SUM(SalesAmount) AS TotalSales
FROM OPENROWSET(
    BULK 'https://synapsestorage2210.blob.core.windows.net/files/data.parquet', --'https://<storage_account>.blob.core.windows.net/<container>/sales.parquet',
    FORMAT = 'PARQUET'
) AS [result]
GROUP BY ProductID;
