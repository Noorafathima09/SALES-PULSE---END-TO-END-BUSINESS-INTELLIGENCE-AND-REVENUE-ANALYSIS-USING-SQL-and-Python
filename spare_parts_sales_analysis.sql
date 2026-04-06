-- =========================================================
-- SPARE PARTS SALES ANALYSIS | SQL PROJECT
-- =========================================================

-- Objective:
-- Analyze multi-branch spare parts and service sales data 
-- to evaluate revenue performance, business trends, and key revenue drivers.


-- =========================================================
-- DATA PREPARATION
-- =========================================================

-- Combine multiple branch datasets into a unified structure
-- to enable consistent cross-branch analysis

CREATE OR REPLACE VIEW sales_all_branches AS
SELECT 
    Branch,
    `Technician Name`,
    `Vehicle Type`,
    `Item Code`,
    `Item Name`,
    `Item Group`,
    Invoice,
    `Posting Date`,
    `Customer Name`,
    Amount,
    Rate,
    `Total`,
    `Total Other Charges`
FROM spare_parts.muttathara

UNION ALL

SELECT 
    Branch,
    `Technician Name`,
    `Vehicle Type`,
    `Item Code`,
    `Item Name`,
    `Item Group`,
    Invoice,
    `Posting Date`,
    `Customer Name`,
    Amount,
    Rate,
    `Total`,
    `Total Other Charges`
FROM spare_parts.palayam;


-- =========================================================
-- DATA CLEANING & FEATURE ENGINEERING
-- =========================================================

-- Transform raw ERP data into a clean, analysis-ready dataset
-- by handling missing values, correcting data types, and
-- removing non-transactional records

CREATE OR REPLACE VIEW sales_final AS
SELECT

    -- Standardize branch labels (handle missing branch values)
    CASE 
        WHEN Branch IS NULL OR TRIM(Branch) = '' THEN 'Counter Sale'
        ELSE Branch
    END AS Branch,

    Invoice,

    -- Convert text date into proper DATE format
    STR_TO_DATE(`Posting Date`, '%Y-%m-%d') AS posting_date,

    -- Convert financial fields to numeric for accurate aggregation
    CAST(Amount AS DECIMAL(12,2)) AS amount,
    CAST(Rate AS DECIMAL(10,2)) AS rate,
    CAST(`Total` AS DECIMAL(12,2)) AS total,

    -- Handle missing or malformed values in additional charges
    CAST(
        CASE 
            WHEN `Total Other Charges` IS NULL 
              OR TRIM(`Total Other Charges`) = '' 
            THEN '0'
            ELSE `Total Other Charges`
        END AS DECIMAL(12,2)
    ) AS other_charges,

    `Item Name`,
    `Item Group`,
    `Vehicle Type`,

    -- Create business category for revenue segmentation
    CASE 
        WHEN `Item Group` LIKE '%Labour%' 
          OR `Item Group` LIKE '%Service%' 
        THEN 'Service'
        ELSE 'Spare Parts'
    END AS category

FROM sales_all_branches

-- Remove invalid and non-transactional rows (e.g., system-generated totals)
WHERE `Posting Date` IS NOT NULL
  AND TRIM(`Posting Date`) <> ''
  AND `Item Name` <> 'Total';


-- =========================================================
-- DATA ANALYSIS
-- =========================================================

-- 1. Branch-Level Revenue Performance
SELECT 
    Branch,
    SUM(amount) AS total_revenue
FROM sales_final
GROUP BY Branch
ORDER BY total_revenue DESC;


-- 2. Monthly Revenue Trend (Time Series Analysis)
SELECT 
    DATE_FORMAT(posting_date, '%Y-%m') AS month,
    SUM(amount) AS revenue
FROM sales_final
GROUP BY month
ORDER BY month;


-- 3. Revenue by Business Category (Service vs Spare Parts)
SELECT 
    category,
    SUM(amount) AS revenue
FROM sales_final
GROUP BY category;


-- 4. Business Model Evaluation (Volume & Value)
SELECT 
    COUNT(DISTINCT Invoice) AS total_invoices,
    SUM(amount) AS total_revenue,
    ROUND(SUM(amount) / COUNT(DISTINCT Invoice), 2) AS avg_invoice_value
FROM sales_final;


-- =========================================================
-- KEY BUSINESS METRICS
-- =========================================================

-- Branch Contribution to Total Revenue
SELECT 
    Branch,
    SUM(amount) AS revenue,
    ROUND(SUM(amount) * 100 / SUM(SUM(amount)) OVER(), 2) AS contribution_pct
FROM sales_final
GROUP BY Branch;


-- Category Contribution to Total Revenue
SELECT 
    category,
    SUM(amount) AS revenue,
    ROUND(SUM(amount) * 100 / SUM(SUM(amount)) OVER(), 2) AS percentage
FROM sales_final
GROUP BY category;