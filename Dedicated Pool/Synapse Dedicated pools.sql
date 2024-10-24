--Create a Table in Dedicated SQL Pool
CREATE TABLE Sales (
    ProductID INT,
    ProductName NVARCHAR(50),
    SalesAmount DECIMAL(18,2),
    SalesDate DATE
)
WITH
    (DISTRIBUTION = HASH(ProductID), HEAP);

--Load Data from Data Lake into Dedicated SQL Pool
COPY INTO Sales
FROM 'https://synapsestorage2210.blob.core.windows.net/files/sales.csv',
WITH (
    FILE_TYPE = 'CSV',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2
);

--Create Table as Select (CTAS)
CREATE TABLE ProductSalesSummary
WITH
    (DISTRIBUTION = ROUND_ROBIN, CLUSTERED COLUMNSTORE INDEX)
AS
SELECT ProductID, SUM(SalesAmount) AS TotalSales
FROM Sales
GROUP BY ProductID;
/**************************************************************************************/
--Bulk Load Data from External Data Source
CREATE EXTERNAL DATA SOURCE MyDataSource
WITH (
    TYPE = HADOOP,
    LOCATION = 'abfss://files@synapsestorage2210.dfs.core.windows.net'		--'abfss://<container>@<storage_account>.dfs.core.windows.net'
);

--Create an external file format
CREATE EXTERNAL FILE FORMAT MyFileFormat
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FIELD_TERMINATOR = ',',
    STRING_DELIMITER = '"',
    DATE_FORMAT = 'yyyy-MM-dd'
);

--Load the data
COPY INTO Sales
FROM 'abfss://<container>@<storage_account>.dfs.core.windows.net/sales/salesdata.csv'
WITH (
    FILE_TYPE = 'CSV',
    FILE_FORMAT = 'MyFileFormat',
    FIRSTROW = 2
);
/***************************************************************************************/

--Upsert Data Using MERGE
MERGE INTO Sales AS target
USING (SELECT ProductID, SalesAmount FROM NewSales) AS source
ON target.ProductID = source.ProductID
WHEN MATCHED THEN
    UPDATE SET target.SalesAmount = target.SalesAmount + source.SalesAmount
WHEN NOT MATCHED BY TARGET THEN
    INSERT (ProductID, SalesAmount) VALUES (source.ProductID, source.SalesAmount);

--Partitioning Tables
CREATE TABLE SalesPartitioned
(
    ProductID INT,
    ProductName NVARCHAR(50),
    SalesAmount DECIMAL(18,2),
    SalesDate DATE
)
WITH
    (DISTRIBUTION = HASH(ProductID),
    PARTITION (SalesDate RANGE RIGHT FOR VALUES ('2023-01-01', '2023-02-01', '2023-03-01')));

--Performing an Aggregation
SELECT ProductID, SUM(SalesAmount) AS TotalSales
FROM Sales
GROUP BY ProductID;

--Joining the Tables
SELECT c.CustomerName, s.ProductID, s.SalesAmount
FROM Customers c
JOIN Sales s ON c.CustomerID = s.CustomerID
WHERE s.SalesAmount > 1000;

--Creating Views for Reusable Queries
CREATE VIEW ProductSalesView AS
SELECT ProductID, SUM(SalesAmount) AS TotalSales
FROM Sales
GROUP BY ProductID;

--Indexing for Query Performance
CREATE CLUSTERED INDEX idx_ProductID ON Sales (ProductID);
