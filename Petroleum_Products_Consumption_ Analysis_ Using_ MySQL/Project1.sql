/* ==============================================================
   PROJECT: Petroleum Consumption Analysis Using MySQL
   DATASET: Government Product Consumption CSV
   AUTHOR: Kunal Sahu
   DESCRIPTION:
       This project analyzes petroleum product consumption trends
       using SQL-based data cleaning and exploratory data analysis.
       It focuses on identifying data quality issues, summarizing
       usage patterns, and finding high-usage months and products.
   ============================================================== */


/* ==============================================================
   STEP 1: DATA LOADING AND INITIAL OVERVIEW
   ============================================================== */

-- View raw data
SELECT * FROM productconsumption;

-- Check total number of rows
SELECT COUNT(*) FROM productconsumption;

-- View distinct product names
SELECT DISTINCT Products FROM productconsumption;


/* ==============================================================
   STEP 2: CREATE CLEAN COPY OF DATA
   ============================================================== */

-- Create a clean table with the same structure
CREATE TABLE petrolpro LIKE productconsumption;

-- Insert all data into the clean table
INSERT INTO petrolpro
SELECT * FROM productconsumption;


/* ==============================================================
   STEP 3: RENAME MISFORMATTED COLUMNS
   ============================================================== */

ALTER TABLE petrolpro RENAME COLUMN `ï»¿"Month"` TO `Months`;
ALTER TABLE petrolpro RENAME COLUMN `Quantity (000 Metric Tonnes)` TO `Quantity`;


/* ==============================================================
   STEP 4: CHECK FOR DUPLICATES
   ============================================================== */

WITH duplicate_cte AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY Months, `Year`, Products, Quantity, updated_date
           ) AS duplicates
    FROM petrolpro
)
SELECT *
FROM duplicate_cte
WHERE duplicates > 1;


/* ==============================================================
   STEP 5: CHECK FOR MISSING VALUES
   ============================================================== */

SELECT *
FROM petrolpro
WHERE 
    Months IS NULL OR
    `Year` IS NULL OR 
    Products IS NULL OR 
    Quantity IS NULL OR 
    updated_date IS NULL;


/* ==============================================================
   STEP 6: CREATE A DATE COLUMN FROM MONTH AND YEAR
   ============================================================== */

SELECT *,
       STR_TO_DATE(CONCAT('01-', Months, '-', Year), '%d-%M-%Y') AS full_date
FROM petrolpro;


/* ==============================================================
   STEP 7: CREATE FINAL CLEAN TABLE WITH DATE COLUMN
   ============================================================== */

CREATE TABLE petrolpro2 (
  Months TEXT,
  Year INT DEFAULT NULL,
  Products TEXT,
  Quantity DOUBLE DEFAULT NULL,
  updated_date TEXT,
  full_date DATE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO petrolpro2
SELECT *,
       STR_TO_DATE(CONCAT('01-', Months, '-', Year), '%d-%M-%Y') AS full_date
FROM petrolpro;

-- Verify table
SELECT * FROM petrolpro2;


/* ==============================================================
   STEP 8: REMOVE REDUNDANT COLUMNS (MONTH, YEAR)
   ============================================================== */

ALTER TABLE petrolpro2 DROP COLUMN Months;
ALTER TABLE petrolpro2 DROP COLUMN Year;

-- Final clean dataset
SELECT * FROM petrolpro2;


/* ==============================================================
   STEP 9: BEGIN EXPLORATORY DATA ANALYSIS (EDA)
   ============================================================== */

-- View all records
SELECT * FROM petrolpro2;

-- Find date range of data
SELECT MAX(full_date) AS latest_date, MIN(full_date) AS earliest_date FROM petrolpro2;

-- Check quantity range
SELECT MAX(Quantity) AS max_quantity, MIN(Quantity) AS min_quantity FROM petrolpro2;


/* ==============================================================
   STEP 10: OUTLIER CHECK
   ============================================================== */

SELECT Products, Quantity, full_date
FROM petrolpro2
WHERE Quantity = 8217.12 OR Quantity = 23.24;


/* ==============================================================
   STEP 11: PRODUCT-WISE TOTAL AND AVERAGE USAGE
   ============================================================== */

WITH total_cte (Products, total_sum, average) AS (
    SELECT 
        Products,
        SUM(Quantity) AS total_sum,
        AVG(Quantity) AS average
    FROM petrolpro2
    GROUP BY Products
    ORDER BY total_sum DESC
)
SELECT * FROM total_cte;


/* Top 5 products by total usage */
WITH total_cte (Products, total_sum, average) AS (
    SELECT 
        Products,
        SUM(Quantity) AS total_sum,
        AVG(Quantity) AS average
    FROM petrolpro2
    GROUP BY Products
    ORDER BY total_sum DESC
)
SELECT * FROM total_cte LIMIT 5;


/* ==============================================================
   STEP 12: MONTHLY USAGE ANALYSIS
   ============================================================== */

SELECT Products, SUM(Quantity) AS total_usage, full_date
FROM petrolpro2
GROUP BY Products, full_date;


/* ==============================================================
   STEP 13: HIGHEST CONSUMPTION MONTH OVERALL
   ============================================================== */

SELECT full_date, SUM(Quantity) AS total_usage
FROM petrolpro2
GROUP BY full_date
ORDER BY total_usage DESC
LIMIT 1;


/* ==============================================================
   STEP 14: PRODUCT-SPECIFIC TREND (e.g., HSD)
   ============================================================== */

WITH usage_cte (total_usage, full_date) AS (
    SELECT SUM(Quantity) AS total_usage, full_date
    FROM petrolpro2
    WHERE Products = 'HSD'
    GROUP BY full_date
)
SELECT * FROM usage_cte ORDER BY total_usage DESC;


/* ==============================================================
   STEP 15: TOP 5 MONTHS OF HIGHEST USAGE PER PRODUCT
   ============================================================== */

WITH usage_cte AS (
    SELECT 
        Products,
        full_date,
        SUM(Quantity) AS total_usage
    FROM petrolpro2
    GROUP BY Products, full_date
)
SELECT *
FROM usage_cte u1
WHERE (
    SELECT COUNT(*) 
    FROM usage_cte u2
    WHERE u2.Products = u1.Products 
      AND u2.total_usage > u1.total_usage
) < 5
ORDER BY Products, total_usage DESC;


/* ==============================================================
   STEP 16: PEAK MONTH FOR EACH PRODUCT
   ============================================================== */

WITH ranked_usage AS (
    SELECT 
        Products,
        full_date,
        SUM(Quantity) AS total_usage,
        ROW_NUMBER() OVER (PARTITION BY Products ORDER BY SUM(Quantity) DESC) AS rank_no
    FROM petrolpro2
    GROUP BY Products, full_date
)
SELECT Products, full_date AS peak_month, total_usage
FROM ranked_usage
WHERE rank_no = 1
ORDER BY total_usage DESC;


/* ==============================================================
   STEP 17: YEARLY CONSUMPTION TRENDS
   ============================================================== */

SELECT 
    YEAR(full_date) AS year,
    Products,
    SUM(Quantity) AS yearly_usage
FROM petrolpro2
GROUP BY year, Products
ORDER BY year, yearly_usage DESC;


/* ==============================================================
   STEP 18: MONTH-OVER-MONTH GROWTH PERCENTAGE
   ============================================================== */

WITH monthly_usage AS (
    SELECT 
        Products,
        full_date,
        SUM(Quantity) AS total_usage,
        LAG(SUM(Quantity)) OVER (PARTITION BY Products ORDER BY full_date) AS prev_month_usage
    FROM petrolpro2
    GROUP BY Products, full_date
)
SELECT 
    Products,
    full_date,
    total_usage,
    ROUND(((total_usage - prev_month_usage) / prev_month_usage) * 100, 2) AS growth_percent
FROM monthly_usage
WHERE prev_month_usage IS NOT NULL;


/* ==============================================================
   STEP 19: COMPARATIVE ANALYSIS BETWEEN PRODUCTS
   ============================================================== */

SELECT 
    full_date,
    SUM(CASE WHEN Products = 'HSD' THEN Quantity ELSE 0 END) AS HSD_usage,
    SUM(CASE WHEN Products = 'MS' THEN Quantity ELSE 0 END) AS MS_usage,
    SUM(CASE WHEN Products = 'SKO' THEN Quantity ELSE 0 END) AS SKO_usage
FROM petrolpro2
GROUP BY full_date
ORDER BY full_date;


/* ==============================================================
   STEP 20: FINAL DATA QUALITY CHECK
   ============================================================== */

SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN Products IS NULL OR Products = '' THEN 1 ELSE 0 END) AS missing_products,
    SUM(CASE WHEN full_date IS NULL THEN 1 ELSE 0 END) AS missing_dates,
    SUM(CASE WHEN Quantity IS NULL THEN 1 ELSE 0 END) AS missing_quantities
FROM petrolpro2;


/* ==============================================================
   CONCLUSION:
   The analysis successfully cleaned and structured the government
   dataset, identified product usage trends, and highlighted peak
   consumption months. MySQL proved effective for EDA tasks such as
   aggregation, trend comparison, and anomaly detection.
   ============================================================== */
