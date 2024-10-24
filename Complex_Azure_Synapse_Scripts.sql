-- 1. AZURE SYNAPSE SERVERLESS SQL POOL SCRIPTS
-- Script 1: Complex data analysis from Parquet files with window functions
SELECT 
    Year,
    Region,
    Product,
    Revenue,
    AVG(Revenue) OVER (PARTITION BY Region ORDER BY Year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as MovingAvgRevenue,
    RANK() OVER (PARTITION BY Region, Year ORDER BY Revenue DESC) as RevenueRank
FROM
    OPENROWSET(
        BULK 'https://yourstorageaccount.dfs.core.windows.net/container/sales/*.parquet',
        FORMAT = 'PARQUET'
    ) WITH (
        Year int,
        Region varchar(50),
        Product varchar(100),
        Revenue decimal(18,2)
    ) as sales
WHERE Year >= 2020;

-- Script 2: JSON processing with external tables
CREATE EXTERNAL TABLE CustomerFeedback (
    FeedbackDate datetime2,
    CustomerId int,
    FeedbackData varchar(MAX)
)
WITH (
    LOCATION = '/feedback/',
    DATA_SOURCE = ExternalDataSource,
    FILE_FORMAT = JSONFileFormat
);

SELECT 
    c.CustomerId,
    JSON_VALUE(c.FeedbackData, '$.rating') as Rating,
    JSON_QUERY(c.FeedbackData, '$.tags') as Tags,
    AVG(CAST(JSON_VALUE(c.FeedbackData, '$.rating') as FLOAT)) OVER 
        (ORDER BY c.FeedbackDate ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) as RollingAvgRating
FROM CustomerFeedback c
WHERE CAST(JSON_VALUE(c.FeedbackData, '$.rating') as INT) > 3;

-- Script 3: Complex geographical data analysis
SELECT 
    g.Region,
    g.City,
    COUNT(*) as TotalEvents,
    ST_Distance(
        ST_Point(g.Longitude, g.Latitude),
        ST_Point(-122.33, 47.61)  -- Seattle coordinates
    ) as DistanceFromSeattle,
    FIRST_VALUE(g.EventType) OVER (
        PARTITION BY g.Region 
        ORDER BY g.EventTimestamp DESC
    ) as LatestEventType
FROM OPENROWSET(
    BULK 'https://yourstorageaccount.dfs.core.windows.net/geo/*.csv',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) WITH (
    Region varchar(50),
    City varchar(100),
    Latitude float,
    Longitude float,
    EventType varchar(50),
    EventTimestamp datetime2
) as g
WHERE g.EventTimestamp >= DATEADD(month, -3, GETDATE())
GROUP BY g.Region, g.City, g.Longitude, g.Latitude, g.EventType, g.EventTimestamp;

-- Script 4: Cross-database querying with dynamic pivot
DECLARE @columns NVARCHAR(MAX), @sql NVARCHAR(MAX);

SELECT @columns = STRING_AGG(QUOTENAME(CategoryName), ',')
FROM (
    SELECT DISTINCT c.CategoryName
    FROM OPENROWSET(
        BULK 'https://yourstorageaccount.dfs.core.windows.net/categories/*.parquet',
        FORMAT = 'PARQUET'
    ) WITH (CategoryName varchar(50)) as c
) as Categories;

SET @sql = N'
SELECT *
FROM (
    SELECT 
        p.ProductName,
        c.CategoryName,
        s.SalesAmount
    FROM OPENROWSET(
        BULK ''https://yourstorageaccount.dfs.core.windows.net/sales/*.parquet'',
        FORMAT = ''PARQUET''
    ) as s
    JOIN Products p ON s.ProductId = p.ProductId
    JOIN Categories c ON p.CategoryId = c.CategoryId
) as SourceTable
PIVOT (
    SUM(SalesAmount)
    FOR CategoryName IN (' + @columns + ')
) as PivotTable;';

EXEC sp_executesql @sql;

-- Script 5: Advanced time series analysis
WITH TimeSeriesData AS (
    SELECT 
        EventTime,
        SensorId,
        Reading,
        LAG(Reading, 1) OVER (PARTITION BY SensorId ORDER BY EventTime) as PrevReading,
        LAG(Reading, 2) OVER (PARTITION BY SensorId ORDER BY EventTime) as PrevReading2,
        LEAD(Reading, 1) OVER (PARTITION BY SensorId ORDER BY EventTime) as NextReading,
        AVG(Reading) OVER (
            PARTITION BY SensorId 
            ORDER BY EventTime 
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) as MovingAvg
    FROM OPENROWSET(
        BULK 'https://yourstorageaccount.dfs.core.windows.net/sensors/*.parquet',
        FORMAT = 'PARQUET'
    ) WITH (
        EventTime datetime2,
        SensorId int,
        Reading float
    ) as s
)
SELECT 
    SensorId,
    EventTime,
    Reading,
    MovingAvg,
    CASE 
        WHEN ABS(Reading - MovingAvg) > 2 * STDEV(Reading) OVER (
            PARTITION BY SensorId 
            ORDER BY EventTime 
            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
        ) THEN 1
        ELSE 0
    END as IsAnomaly
FROM TimeSeriesData
WHERE EventTime >= DATEADD(day, -7, GETDATE());

-- 2. DEDICATED SQL POOL SCRIPTS
-- Script 1: Materialized view with distribution
CREATE MATERIALIZED VIEW SalesAnalysis
WITH (
    DISTRIBUTION = HASH(CustomerID),
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT 
    c.CustomerID,
    c.CustomerSegment,
    p.ProductCategory,
    s.OrderDate,
    s.ShipDate,
    s.SalesAmount,
    s.Quantity,
    DATEDIFF(day, s.OrderDate, s.ShipDate) as ProcessingTime,
    ROW_NUMBER() OVER (
        PARTITION BY c.CustomerSegment 
        ORDER BY s.SalesAmount DESC
    ) as RankInSegment
FROM 
    FactSales s
    JOIN DimCustomer c ON s.CustomerID = c.CustomerID
    JOIN DimProduct p ON s.ProductID = p.ProductID
WHERE 
    s.OrderDate >= DATEADD(year, -2, GETDATE());

-- Script 2: Complex partition switching
CREATE TABLE StagingSales
WITH (
    DISTRIBUTION = HASH(OrderID),
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT * FROM FactSales WHERE 1 = 2;

CREATE PROCEDURE LoadSalesPartition
    @StartDate date,
    @EndDate date
AS
BEGIN
    BEGIN TRANSACTION;
    
    -- Load new data into staging
    INSERT INTO StagingSales
    SELECT * FROM ExternalSalesSource
    WHERE OrderDate >= @StartDate 
    AND OrderDate < @EndDate;

    -- Switch out old partition
    ALTER TABLE FactSales SWITCH PARTITION $Partition.SalesDateRange(@StartDate)
    TO StagingSales PARTITION $Partition.SalesDateRange(@StartDate);

    -- Switch in new partition
    ALTER TABLE StagingSales SWITCH PARTITION $Partition.SalesDateRange(@StartDate)
    TO FactSales PARTITION $Partition.SalesDateRange(@StartDate);

    COMMIT TRANSACTION;
END;

-- Script 3: Optimized aggregate calculation
CREATE TABLE #TempResults
WITH (
    DISTRIBUTION = ROUND_ROBIN,
    HEAP
)
AS
SELECT 
    c.CustomerSegment,
    p.ProductCategory,
    d.FiscalYear,
    d.FiscalQuarter,
    COUNT(DISTINCT s.CustomerID) as UniqueCustomers,
    SUM(s.SalesAmount) as TotalSales,
    AVG(s.SalesAmount) as AvgSalesAmount
FROM 
    FactSales s
    JOIN DimCustomer c ON s.CustomerID = c.CustomerID
    JOIN DimProduct p ON s.ProductID = p.ProductID
    JOIN DimDate d ON s.OrderDate = d.DateKey
WHERE 
    d.FiscalYear >= 2020
GROUP BY 
    GROUPING SETS (
        (c.CustomerSegment, p.ProductCategory, d.FiscalYear, d.FiscalQuarter),
        (c.CustomerSegment, p.ProductCategory, d.FiscalYear),
        (c.CustomerSegment, d.FiscalYear),
        (p.ProductCategory, d.FiscalYear)
    );

-- Script 4: Dynamic CTAS with statistics
DECLARE @SqlScript nvarchar(max);
SET @SqlScript = N'
CREATE TABLE CustomerMetrics
WITH (
    DISTRIBUTION = HASH(CustomerID),
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT 
    c.CustomerID,
    c.CustomerSegment,
    COUNT(DISTINCT s.OrderID) as TotalOrders,
    SUM(s.SalesAmount) as LifetimeValue,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.SalesAmount) 
        OVER (PARTITION BY c.CustomerSegment) as MedianOrderValue,
    DATEDIFF(day, MIN(s.OrderDate), MAX(s.OrderDate)) as CustomerTenureDays
FROM 
    DimCustomer c
    JOIN FactSales s ON c.CustomerID = s.CustomerID
GROUP BY 
    c.CustomerID,
    c.CustomerSegment;';

EXEC sp_executesql @SqlScript;

CREATE STATISTICS stats_CustomerMetrics_CustomerSegment 
ON CustomerMetrics(CustomerSegment) 
WITH FULLSCAN;

-- Script 5: Complex recursive query
WITH RECURSIVE ProductHierarchy AS (
    -- Base case
    SELECT 
        ProductID,
        ParentProductID,
        ProductName,
        ProductLevel,
        CAST(ProductName as varchar(1000)) as Hierarchy,
        CAST(ProductID as varchar(100)) as HierarchyID
    FROM DimProduct
    WHERE ParentProductID IS NULL

    UNION ALL

    -- Recursive case
    SELECT 
        p.ProductID,
        p.ParentProductID,
        p.ProductName,
        p.ProductLevel,
        CAST(ph.Hierarchy + ' > ' + p.ProductName as varchar(1000)),
        CAST(ph.HierarchyID + '.' + CAST(p.ProductID as varchar(10)) as varchar(100))
    FROM DimProduct p
    JOIN ProductHierarchy ph ON p.ParentProductID = ph.ProductID
)
SELECT 
    HierarchyID,
    Hierarchy,
    ProductLevel,
    COUNT(*) OVER (PARTITION BY ProductLevel) as ProductsAtLevel,
    ROW_NUMBER() OVER (ORDER BY HierarchyID) as HierarchyRank
FROM ProductHierarchy
ORDER BY HierarchyID;

-- 3. SPARK SQL POOL SCRIPTS
-- Script 1: Complex data transformation with window functions
%%sql
WITH UserBehavior AS (
    SELECT 
        user_id,
        event_type,
        event_timestamp,
        LEAD(event_timestamp) OVER (
            PARTITION BY user_id 
            ORDER BY event_timestamp
        ) as next_event_timestamp,
        LAG(event_type) OVER (
            PARTITION BY user_id 
            ORDER BY event_timestamp
        ) as previous_event
    FROM events
    WHERE date_partition >= date_sub(current_date(), 30)
)
SELECT 
    user_id,
    event_type,
    COUNT(*) as event_count,
    AVG(unix_timestamp(next_event_timestamp) - unix_timestamp(event_timestamp)) as avg_time_to_next_event,
    COLLECT_LIST(previous_event) as event_sequence
FROM UserBehavior
GROUP BY user_id, event_type
HAVING COUNT(*) > 10;

-- Script 2: Machine learning feature preparation
%%sql
CREATE OR REPLACE TEMPORARY VIEW feature_matrix AS
WITH user_metrics AS (
    SELECT 
        user_id,
        COUNT(DISTINCT session_id) as total_sessions,
        SUM(case when event_type = 'purchase' then 1 else 0 end) as purchase_count,
        AVG(session_duration) as avg_session_duration,
        PERCENTILE_APPROX(items_viewed, 0.5) as median_items_viewed
    FROM user_sessions
    GROUP BY user_id
),
user_categories AS (
    SELECT 
        user_id,
        category_id,
        COUNT(*) as category_views,
        ROW_NUMBER() OVER (
            PARTITION BY user_id 
            ORDER BY COUNT(*) DESC
        ) as category_rank
    FROM item_views
    GROUP BY user_id, category_id
)
SELECT 
    um.*,
    map_from_arrays(
        COLLECT_LIST(uc.category_id),
        COLLECT_LIST(uc.category_views)
    ) as category_view_map
FROM user_metrics um
LEFT JOIN user_categories uc ON um.user_id = uc.user_id
WHERE uc.category_rank <= 5
GROUP BY 
    um.user_id,
    um.total_sessions,
    um.purchase_count,
    um.avg_session_duration,
    um.median_items_viewed;

-- Script 3: Advanced time series analysis
%%sql
WITH hourly_metrics AS (
    SELECT 
        date_trunc('hour', event_timestamp) as hour,
        COUNT(*) as events,
        COUNT(DISTINCT user_id) as unique_users,
        SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END) as revenue
    FROM events
    WHERE date_partition >= date_sub(current_date(), 7)
    GROUP BY date_trunc('hour', event_timestamp)
),
metrics_with_stats AS (
    SELECT 
        *,
        AVG(events) OVER w as avg_events,
        STDDEV(events) OVER w as stddev_events,
        AVG(revenue) OVER w as avg_revenue,
        STDDEV(revenue) OVER w as stddev_revenue
    FROM hourly_metrics
    WINDOW w AS (
        ORDER BY hour 
        ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
    )
)
SELECT 
    hour,
    events,
    revenue,
    CASE 
        WHEN events > avg_events + 2 * stddev_events THEN 'High'
        WHEN events < avg_events - 2 * stddev_events THEN 'Low'
        ELSE 'Normal'
    END as event_anomaly,
    CASE 
        WHEN revenue > avg_revenue + 2 * stddev_revenue THEN 'High'
        WHEN revenue < avg_revenue - 2 * stddev_revenue THEN 'Low'
        ELSE 'Normal'
    END as revenue_anomaly
FROM metrics_with_stats
ORDER BY hour;

-- Script 4: Complex aggregation with cube
%%sql
SELECT 
    COALESCE(country, 'All Countries') as country,
    COALESCE(device_type, 'All Devices') as device_type,
    COALESCE(user_segment, 'All Segments') as user_segment,
    COUNT(DISTINCT user_id) as unique_users,
    SUM(session_duration) as total_duration,
    AVG(session_duration) as avg_duration,
    APPROX_PERCENTILE(session_duration, 0.5) as median_duration,
    SUM(page_views) as total_page_views,
    SUM(CASE WHEN conversion_flag = true THEN 1 ELSE 0 END) as conversions,
    SUM(CASE WHEN conversion_flag = true THEN 1 ELSE 0 END) / COUNT(DISTINCT user_id) as conversion_rate,
    SUM(revenue) as total_revenue,
    SUM(revenue) / COUNT(DISTINCT user_id) as revenue_per_user
FROM user_session_metrics
WHERE date_partition >= date_sub(current_date(), 90)
GROUP BY 
    CUBE(country, device_type, user_segment)
HAVING COUNT(DISTINCT user_id) >= 100
ORDER BY 
    country NULLS FIRST,
    device_type NULLS FIRST,
    user_segment NULLS FIRST;

-- Script 5: Advanced cohort analysis with retention metrics
%%sql
WITH first_purchase AS (
    -- Get first purchase date for each user
    SELECT 
        user_id,
        DATE_TRUNC('month', MIN(purchase_date)) as cohort_month
    FROM purchase_events
    WHERE date_partition >= date_sub(current_date(), 365)
    GROUP BY user_id
),
monthly_activity AS (
    -- Calculate activity for each user by month
    SELECT 
        user_id,
        DATE_TRUNC('month', activity_date) as activity_month,
        SUM(purchase_amount) as monthly_spend,
        COUNT(DISTINCT session_id) as session_count,
        SUM(items_purchased) as items_count
    FROM user_activity
    WHERE date_partition >= date_sub(current_date(), 365)
    GROUP BY user_id, DATE_TRUNC('month', activity_date)
),
cohort_metrics AS (
    -- Join first purchase with monthly activity
    SELECT 
        fp.cohort_month,
        ma.activity_month,
        MONTHS_BETWEEN(ma.activity_month, fp.cohort_month) as month_number,
        COUNT(DISTINCT fp.user_id) as cohort_size,
        COUNT(DISTINCT ma.user_id) as active_users,
        SUM(ma.monthly_spend) as total_spend,
        SUM(ma.session_count) as total_sessions,
        SUM(ma.items_count) as total_items
    FROM first_purchase fp
    LEFT JOIN monthly_activity ma ON fp.user_id = ma.user_id
    GROUP BY 
        fp.cohort_month,
        ma.activity_month
),
retention_metrics AS (
    -- Calculate retention metrics
    SELECT 
        cohort_month,
        activity_month,
        month_number,
        cohort_size,
        active_users,
        total_spend,
        total_sessions,
        total_items,
        ROUND(active_users / cohort_size * 100, 2) as retention_rate,
        ROUND(total_spend / active_users, 2) as spend_per_active_user,
        ROUND(total_sessions / active_users, 2) as sessions_per_active_user,
        ROUND(total_items / active_users, 2) as items_per_active_user,
        ROUND(total_spend / total_sessions, 2) as spend_per_session
    FROM cohort_metrics
    WHERE month_number >= 0
),
cohort_comparison AS (
    -- Compare cohorts performance
    SELECT 
        cohort_month,
        month_number,
        retention_rate,
        spend_per_active_user,
        AVG(retention_rate) OVER (
            PARTITION BY month_number
            ORDER BY cohort_month 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as rolling_avg_retention,
        retention_rate - LAG(retention_rate) OVER (
            PARTITION BY month_number 
            ORDER BY cohort_month
        ) as retention_change,
        RANK() OVER (
            PARTITION BY month_number 
            ORDER BY retention_rate DESC
        ) as retention_rank
    FROM retention_metrics
)
SELECT 
    *,
    CASE 
        WHEN retention_rate > rolling_avg_retention + 10 THEN 'Outstanding'
        WHEN retention_rate > rolling_avg_retention THEN 'Above Average'
        WHEN retention_rate < rolling_avg_retention - 10 THEN 'Needs Improvement'
        ELSE 'Average'
    END as cohort_performance
FROM cohort_comparison
WHERE cohort_month >= date_sub(current_date(), 365)
ORDER BY 
    cohort_month,
    month_number;

