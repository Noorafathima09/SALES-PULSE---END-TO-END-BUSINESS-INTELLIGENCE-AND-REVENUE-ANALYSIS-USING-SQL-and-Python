-- ==================================================================================================================
  -- SALES PULSE - END-TO-END BUSINESS INTELLIGENCE AND REVENUE ANALYSIS USING SQL 
-- ==================================================================================================================


--  ==================================================================================================================
-- Project Overview:
-- This project analyzes multi-branch spare parts and service sales data to evaluate revenue performance, operational efficiency, customer demand patterns, and business risk. 
-- Using SQL, the analysis progresses from raw data consolidation and cleaning to profitability analysis, KPI design, and decision-ready business recommendations.
--  ==================================================================================================================





-- STEP(1)[DATA SOURCE & STRUCTURE UNDERSTANDING] ----

-- (1.1) what data sources are included in this project? ---
SHOW DATABASES;
USE spare_parts;
SHOW TABLES;

-- (1.2) Does each data source represents the same business process and granularity? Are the schemas consistent across the branches? ---
 DESCRIBE spare_parts.palayam;
 DESCRIBE spare_parts.muttathara;
 
SHOW TABLES;

-- (1.3) How should schema differences be handled to enable cross branch analysis? 

-- Schema inspection revealed that while both branch tables represent the same item-level sales transaction process, some operational columns are present
-- only in specific branches (e.g., Mode of Payment in Muttathara and  Receivable/Account fields in Palayam).

-- These differences were treated as structural variations rather than data quality issues.
-- To enable reliable cross-branch analysis, a unified schema was designed by explicitly aligning all common columns and preserving branch-specific fields using NULL values where data was unavailable.

-- This approach ensures schema consistency while maintaining analytical flexibility for branch-specific attributes.



-- (1.4) How can branch data be unified into a single analytical dataset?---

CREATE OR REPLACE VIEW sales_all_branches AS

SELECT
    Branch,
    `Technician Name`,
    `Vehicle Type`,
    `Item Code`,
    `Item Name`,
    `Item Group`,
    Description,
    Invoice,
    `Posting Date`,
    `Customer Group`,
    Customer,
    `Customer Name`,
    `Mode Of Payment`,
    NULL AS `Receivable Account`,
    NULL AS Company,
    NULL AS `Income Account`,
    NULL AS `Cost Center`,
    `Stock Qty`,
    `Stock UOM`,
    Rate,
    Amount,
    `Output Tax CGST Rate`,
    `Output Tax CGST Amount`,
    `Output Tax SGST Rate`,
	`Output Tax SGST Amount`,
    `Total Tax`,
    `Total Other Charges`,
    Total
FROM spare_parts.muttathara

UNION ALL

SELECT
    Branch,
    `Technician Name`,
    `Vehicle Type`,
    `Item Code`,
    `Item Name`,
    `Item Group`,
    NULL AS Description,
    Invoice,
    `Posting Date`,
    `Customer Group`,
    Customer,
    `Customer Name`,
    NULL AS `Mode Of Payment`,
    `Receivable Account`,
    Company,
    `Income Account`,
    `Cost Center`,
     `Stock Qty`,
    `Stock UOM`,
    Rate,
    Amount,
    `Output Tax CGST Rate`,
    `Output Tax CGST Amount`,
    `Output Tax SGST Rate`,
    `Output Tax SGST Amount`,
    `Total Tax`,
    `Total Other Charges`,
    Total
FROM spare_parts.palayam;

-- Validate that all source rows are preserved in the unified dataset
SELECT
  (SELECT COUNT(*) FROM spare_parts.muttathara) +
  (SELECT COUNT(*) FROM spare_parts.palayam) AS source_row_count,
  (SELECT COUNT(*) FROM sales_all_branches) AS unified_row_count;
-- This unified view establishes a single, consistent analytical dataset
-- by aligning branch-level schemas and preserving all transaction records.
-- It serves as the foundational data layer for all subsequent profiling,
-- cleaning, and analytical steps in this project.

DESCRIBE sales_all_branches;

SELECT COUNT(*) FROM sales_all_branches;

-- [INSIGHT]
-- • I consolidated sales data from multiple branch-specific tables in SQL. After verifying that each table represented item-level transactions, I aligned schemas across branches, handling branch-specific columns with NULLs to ensure consistency. 
-- • Finally, I unified all data into a single SQL view, creating a clean, analysis-ready dataset for cross-branch insights.


-- STEP 2 [INITIAL DATA PROFILING AND QUALITY AUDIT]--

-- • Purpose of Step 2:
-- • To understand data reliability, risks, and limitations before cleaning or analysis.
-- • This step answers:
-- • “Can this data be trusted for business decisions?”


-- (2.1) How many total transactions exist across all branches?--

SELECT COUNT(*) AS total_transactions
FROM sales_all_branches;
-- I first quantified the dataset to understand its overall size and analytical scope--

-- (2.2) How many transactions come from each branch?-- 

SELECT
    Branch,
    COUNT(*) AS transactions
FROM sales_all_branches
GROUP BY Branch;
--  Branch-level distribution revealed differences in transaction volume,requiring normalization in later analysis


-- (2.3) Are there any invalid or missing values in key fields (such as posting date ) that could affect analysis?--

-- Check for non-convertible date values (Muttathara)
SELECT COUNT(*) AS bad_rows
FROM spare_parts.muttathara
WHERE `Posting Date` IS NOT NULL
AND STR_TO_DATE(Posting Date, '%Y-%m-%d') IS NULL;

-- Identify empty / summary row
SELECT COUNT(*) AS summary_rows
FROM spare_parts.muttathara
WHERE total IS NOT NULL
AND (`Posting Date` IS NULL OR TRIM(`Posting Date`) = '');

SELECT
  SUM(Total IS NULL OR TRIM(Total)='') AS empty_total,
  SUM(Rate IS NULL OR TRIM(Rate)='') AS empty_rate,
  SUM(Amount IS NULL OR TRIM(Amount)='') AS empty_amount
FROM sales_all_branches;

SELECT
  COUNT(*) AS total_rows,
  SUM(`Mode Of Payment` IS NULL OR TRIM(`Mode Of Payment`)='') AS missing_mode_payment
FROM sales_all_branches;

-- Mode of Payment has very low completeness due to upstream process design

-- 2.4 Data type risk assessment -- 

DESCRIBE sales_all_branches;
-- Several numeric and date fields were stored as text, posing risks to aggregation and filtering accuracy 


-- (2.5) What time period does the dataset cover?--

SELECT
    MIN(`Posting Date`) AS start_date,
    MAX(`Posting Date`) AS end_date
FROM sales_all_branches;


-- (2.6) What categorical values exist in Item Group, and how are they distributed across branches?

-- Explore unique Item Groups
SELECT DISTINCT `Item Group`
FROM sales_all_branches
ORDER BY `Item Group`;

-- Analyze Item Group distribution by branch
SELECT
  Branch,
  `Item Group`,
  COUNT(*) AS transaction_count
FROM sales_all_branches
GROUP BY Branch, `Item Group`
ORDER BY Branch, transaction_count DESC;

-- [INSIGHT]
-- Item Group exploration revealed that the dataset contains both service-related and spare-part-related transactions within a single column.
-- Branch-level distribution analysis showed variation in the mix of services and spare parts, indicating that direct sales comparisons require additional feature engineering.
-- These observations informed the creation of a derived Item Category field in the subsequent data cleaning and feature engineering step.

SELECT
  Branch,
  CASE
    WHEN `Item Group` LIKE '%Labour%' OR `Item Group` LIKE '%Service%' THEN 'Service'
    ELSE 'Spare Part'
  END AS Item_Category,
  COUNT(*) AS transaction_count
FROM sales_all_branches
GROUP BY Branch, Item_Category
ORDER BY Branch, transaction_count DESC;

-- (2.7) Granularity and Invoice structure validation

SELECT
  Invoice,
  COUNT(*) AS line_items
FROM sales_all_branches
GROUP BY Invoice
HAVING COUNT(*) > 1;


-- Step 2 Summary:
-- • Posting Date had non-convertible and empty values, plus a summary row.
-- • Numeric fields are stored as text; may require type conversion.
-- • mode of payment has low completeness due to upstream process design
-- • Item Group mixes service and spare-part items.
-- • Data is line-item level with multiple rows per invoice.
-- • These findings will guide the data cleaning and feature engineering.


-- STEP 3 [ DATA CLEANING, STANDARDIZATION AND FEATURE ENGINEERING ] --

-- The objective of Step 3 is to:

-- • Correct critical data integrity issues
-- • Standardize data types for accurate calculations
-- • Engineer analytical features required for business insights
-- • Preserve real-world business behavior without fabricating data
-- • This step transforms the dataset from raw ERP output into an analysis-ready analytical dataset.

-- (3.1) Remove Non-Transactional/Bad rows--

-- Validate non-transactional rows before deletion
SELECT *
FROM spare_parts.muttathara
WHERE Total IS NOT NULL
  AND (`Posting Date` IS NULL OR TRIM(`Posting Date`) = '');


DELETE
FROM spare_parts.muttathara
WHERE Total IS NOT NULL
  AND (`Posting Date` IS NULL OR TRIM(`Posting Date`) = '');

DELETE
FROM spare_parts.palayam
WHERE Total IS NOT NULL
  AND (`Posting Date` IS NULL OR TRIM(`Posting Date`) = '');

-- Non-transactional summary rows were identified and removed to ensure that each row in the dataset represents a valid invoice line item.

-- (3.2) Standardize date data type--

ALTER TABLE spare_parts.muttathara
MODIFY COLUMN `Posting Date` DATE;

ALTER TABLE spare_parts.palayam
MODIFY COLUMN `Posting Date` DATE;

-- Date fields were standardized to ensure reliable time-series analysis and accurate financial reporting --

-- (3.3) Handling of Incomplete Mode of Payment --

-- Columns with structural missingness were preserved without imputation to avoid introducing artificial business behavior.
-- Mode Of Payment is structurally incomplete (~90% missing) 
-- indicating payments are captured in a separate system.
-- Column retained for reference but excluded from financial calculations.

-- (3.4) Standardization of Numeric Fields --

ALTER TABLE spare_parts.muttathara
MODIFY COLUMN Rate DECIMAL(10,2),
MODIFY COLUMN Amount DECIMAL(12,2),
MODIFY COLUMN `Total Tax` DECIMAL(12,2),
MODIFY COLUMN Total DECIMAL(12,2);

ALTER TABLE spare_parts.palayam
MODIFY COLUMN Rate DECIMAL(10,2),
MODIFY COLUMN Amount DECIMAL(12,2),
MODIFY COLUMN `Total Tax` DECIMAL(12,2),
MODIFY COLUMN Total DECIMAL(12,2);

-- Clean malformed Total Other Charges values
UPDATE spare_parts.muttathara
SET `Total Other Charges` = '0'
WHERE `Total Other Charges` IS NULL
   OR TRIM(`Total Other Charges`) = '';

UPDATE spare_parts.palayam
SET `Total Other Charges` = '0'
WHERE `Total Other Charges` IS NULL
   OR TRIM(`Total Other Charges`) = '';

-- Convert Total Other Charges to numeric
ALTER TABLE spare_parts.muttathara
MODIFY COLUMN `Total Other Charges` DECIMAL(12,2);

ALTER TABLE spare_parts.palayam
MODIFY COLUMN `Total Other Charges` DECIMAL(12,2);

-- Financial fields were standardized to numeric types to ensure calculation accuracy and reporting reliability--

-- (3.5) Feature Engineering:Service vs Spare part 

CREATE OR REPLACE VIEW sales_all_branches_clean AS
SELECT *,
  CASE
    WHEN `Item Group` LIKE '%Labour%'
      OR `Item Group` LIKE '%Service%'
    THEN 'Service'
    ELSE 'Spare Part'
  END AS Item_Category
FROM sales_all_branches;

-- A derived Item Category feature was engineered to separate service revenue from spare-part sales, enabling accurate business segmentation --

-- (3.6) Validation Of Invoice-Level Granularity --

SELECT
  Invoice,
  COUNT(*) AS line_items
FROM sales_all_branches_clean
GROUP BY Invoice
HAVING COUNT(*) > 1;

-- Data granularity was validated to confirm that each record represents an invoice line item, ensuring correct aggregation logic in subsequent analyses--

-- (3.7) Outlier Handling Strategy--

-- Outliers were retained after validation, as they represent genuine business transactions rather than data errors.

-- (3.8) Standardization of Missing Branch Identifiers--

CREATE OR REPLACE VIEW spare_parts.sales_all_branches_clean_labeled AS
SELECT
    CASE
        WHEN Branch IS NULL OR TRIM(Branch) = '' 
            THEN 'Counter sale'
        ELSE Branch
    END AS Branch,

    `Technician Name`,
    `Vehicle Type`,
    `Item Code`,
    `Item Name`,
    `Item Group`,
    Description,
    Invoice,
    `Posting Date`,
    `Customer Group`,
    Customer,
    `Customer Name`,
    `Receivable Account`,
    Company,
    `Income Account`,
    `Cost Center`,
    `Mode Of Payment`,
    `Stock Qty`,
    `Stock UOM`,
    Rate,
    Amount,
    `Output Tax CGST Rate`,
    `Output Tax CGST Amount`,
    `Output Tax SGST Rate`,
    `Output Tax SGST Amount`,
    `Total Tax`,
    `Total Other Charges`,
    Total,
      CASE
        WHEN `Item Group` LIKE '%Labour%'
          OR `Item Group` LIKE '%Service%'
        THEN 'Service'
        ELSE 'Spare Part'
    END AS Item_Category
FROM spare_parts.sales_all_branches_clean;
 
-- Some transactions lacked branch identifiers because they represent direct counter sales.
-- These were standardized by assigning a consistent 'Counter Sale' label,
-- ensuring accurate branch-level aggregation and reliable downstream analysis.



-- (3.9) Final view--

CREATE OR REPLACE VIEW sales_all_branches_final AS
SELECT
    Branch,
    `Technician Name`,
    `Vehicle Type`,
    `Item Code`,
    `Item Name`,
    `Item Group`,
    Description,
    Invoice,
    `Posting Date`,
    `Customer Group`,
    Customer,
    `Customer Name`,
    `Receivable Account`,
    Company,
    `Income Account`,
    `Cost Center`,
    `Mode Of Payment`,
    `Stock Qty`,
    `Stock UOM`,
    Rate,
    Amount,
    `Output Tax CGST Rate`,
    `Output Tax CGST Amount`,
    `Output Tax SGST Rate`,
    `Output Tax SGST Amount`,
    `Total Tax`,
    `Total Other Charges`,
    Total,
    Item_Category
FROM spare_parts.sales_all_branches_clean_labeled;


-- Final view for clean and practical further analysis created 

-- Step 3 Summary ;

-- • Raw ERP sales data was transformed into a clean, analysis-ready dataset while preserving real business behavior.
-- •  Non-transactional rows were removed, dates and financials standardized, and branch identifiers unified. Missing fields were documented, and outliers retained as genuine high-value transactions.
-- • An Item Category feature was engineered to separate service and spare-part revenue.
-- • The final analytical view (sales_all_branches_final) provides a reliable, audit-ready foundation for all performance and revenue analysis.



-- STEP 4 [BUSINESS PERFORMANCE & REVENUE ANALYSIS]

-- The objective of Step 4 is to;

-- • evaluate business performance across branches, time, and categories using clean, standardized data.
-- • This step answers the question:
-- • “How is the business performing, where is revenue coming from, and how do branches differ?”

-- (4.1) Overall Business Performance 
-- What is the overall scale of the business in terms of revenue and volume?

SELECT
    COUNT(DISTINCT Invoice) AS total_invoices,
    COUNT(*) AS total_line_items,
    SUM(Total) AS total_revenue
FROM sales_all_branches_final;


-- This defines the size of the business problem before drilling deeper.
-- Establishes the overall scale, transaction volume, and revenue size before segmentation

-- [INSIGHT]
	-- The business processed 16,431 invoices totaling ₹17.42M across 70,764 line items, highlighting a high-volume, multi-item sales environment.
    -- This baseline reflects operational depth, consistent demand, and sets the stage for analyzing branch efficiency, revenue mix, and growth trends.
    
    
-- (4.2) Monthly Revenue trend --
-- How does revenue evolve month over month ? 

SELECT
    DATE_FORMAT(Posting Date, '%Y-%m') AS `year month`,
    SUM(Total) AS monthly_revenue
FROM sales_all_branches_final
GROUP BY DATE_FORMAT(Posting Date, '%Y-%m')
ORDER BY DATE_FORMAT(Posting Date, '%Y-%m');

-- Revenue displays clear month-by-month variation, reflecting real operational seasonality rather than random fluctuation. 
-- This trend analysis provides a reliable basis for forecasting, capacity planning, and performance benchmarking.

-- [INSIGHT] 
  -- Revenue remained stable at ₹2.5–3M/month from July–December 2025, peaking at ₹2.948M in December due to year-end demand. January 2026 appears lower (₹0.749M) as it’s a partial month.
  -- Trend analysis highlights seasonal patterns and peak periods, supporting data-driven sales forecasting and resource planning.


-- (4.3) Branch level Revenue Performance --
-- Which branches contribute most to total revenue?

SELECT
    Branch,
    SUM(Total) AS branch_revenue
FROM sales_all_branches_final
GROUP BY Branch
ORDER BY branch_revenue DESC;


-- Revenue contribution is uneven across branches, with specific locations driving a dominant share of total sales. 
-- This highlights opportunities for targeted optimization, replication of best practices, and risk diversification.


-- [INSIGHT] 
  -- Palayam leads with ₹9.19M, followed by Muttathara at ₹6.12M,
-- while Counter Sales contribute ₹2.11M.
-- This highlights branch leadership, the revenue significance of counter sales,
-- and opportunities to optimize walk-in customer monetization.



-- (4.4) Monthly Revenue by Branch -- 
-- How does each branch perform over time? --

SELECT
    Branch,
    DATE_FORMAT(Posting Date, '%Y-%m') AS `year month`,
    SUM(Total) AS monthly_revenue
FROM sales_all_branches_final
GROUP BY Branch, `year month`
ORDER BY Branch, `year month`;


-- Branch-level monthly analysis reveals stable revenue patterns for primary branches, while system-unassigned transactions remain consistent but comparatively smaller.
-- This confirms that missing branch attribution does not distort core performance metrics and has been correctly isolated.


-- [INSIGHT] 
-- Palayam consistently leads monthly sales, peaking in December 2025,
-- while Muttathara shows stable, predictable performance.
-- Counter Sales contribute a smaller but consistent revenue stream,
-- reflecting steady walk-in demand.
-- January 2026’s dip reflects a partial month, not operational decline.


 

-- (4.5) Services vs Spare parts performance --
-- How much revenue comes from services vs spare parts, and how dependent is the business on each stream ?--

SELECT
    Item_Category,
    COUNT(DISTINCT Invoice) AS invoices,
    SUM(`Stock Qty`) AS units_sold,
    SUM(Total) AS revenue,
    ROUND(
        100 * SUM(Total) / SUM(SUM(Total)) OVER (),
        2
    ) AS revenue_share_pct
FROM sales_all_branches_final
GROUP BY Item_Category
ORDER BY revenue DESC;

--  The revenue mix shows that spare parts generate the majority of total revenue, while services contribute a smaller but significant share. 
-- This indicates a product-driven revenue model with service support potential.

-- [INSIGHTS]
--  Spare parts drive 75% of revenue (₹13.04M across 8.4K transactions), forming the business’s financial backbone. 
--  Services match in volume (8.2K jobs) but contribute 25% of revenue (₹4.38M), highlighting lower per-job value. 
--  This mix shows opportunities to optimize service pricing, upsell, or bundle with parts to boost revenue while leveraging an already strong customer base.


-- Step 4 summary;

-- • Cleaned ERP data was transformed into actionable insights. 
-- • The business processed 16K+ invoices and ₹17.4M+ revenue, with Palayam driving over half of sales and Muttathara showing stable performance. 
-- • Spare parts account for 75% of revenue, while services engage customers despite lower per-transaction value. 
-- • Step 4 highlights key revenue drivers, operational patterns, and strategic opportunities, providing a solid foundation for profitability and efficiency analysis.




-- STEP 5 [PROFITABILITY AND OPERATIONAL EFFICIENCY ANALYSIS]

-- •  The objective of step 5 is to ;
-- • Evaluate efficiency,pricing behavior, transaction quality, and operational leverage using revenue-per unit and revenue-per-invoice metrics

-- (5.1) Average Revenue per Invoice --
-- Are invoices high-value or volume-driven?

SELECT
    COUNT(DISTINCT Invoice) AS total_invoices,
    SUM(Total) AS total_revenue,
    ROUND(SUM(Total) / COUNT(DISTINCT Invoice), 2) AS avg_revenue_per_invoice
FROM sales_all_branches_final;

-- -- Shows billing strength, distinguishes between many small bills and fewer high-value bills,
-- and is critical for pricing and upsell strategy.

 
 -- [INSIGHT]
-- The business processed 16.4K invoices totaling ₹17.42M, averaging ₹1.06K per invoice.
-- This high-volume, mid-value model reflects steady customer flow and operational efficiency.
--  Improving average invoice value through upselling or service-part bundling could drive significant revenue growth, providing a strong basis for pricing and sales strategy analysis.


-- (5.2) Average Revenue per Line Item 
-- How much value does each sold item or service generate?

SELECT
    COUNT(*) AS total_line_items,
    SUM(Total) AS total_revenue,
    ROUND(SUM(Total) / COUNT(*), 2) AS avg_revenue_per_line_item
FROM sales_all_branches_final;

-- Reveals Pricing depth,Discount pressure,Item level monetization strength

-- [INSIGHT] 

-- Revenue of ₹17.42M across 70.8K line items averages ₹246 per item, confirming a high-frequency, mid-value sales model.
-- The gap with the average invoice (~₹1.06K) shows customers buy multiple items per invoice, reflecting effective cross-selling.
--  Strategically, optimizing per-line pricing, bundling, or value-added services can drive significant revenue growth at scale.



-- (5.3) Branch Efficiency-Revenue per invoice
-- Which branches bill more effectively per customer visit?

SELECT
    Branch,
    COUNT(DISTINCT Invoice) AS invoices,
    SUM(Total) AS revenue,
    ROUND(SUM(Total) / COUNT(DISTINCT Invoice), 2) AS revenue_per_invoice
FROM sales_all_branches_final
GROUP BY Branch
ORDER BY revenue_per_invoice DESC;

-- Separates busy branches from smart branches

-- [INSIGHT]
-- Muttathara leads in average invoice value (₹1,089) with fewer invoices, reflecting higher-value or service-heavy transactions. 
-- Palayam drives volume (10.3K invoices, ₹9.19M) with lower average value (₹896), showing a high-footfall, fast-moving retail model
-- Counter Sales show higher average invoice value (₹3,791),reflecting bulk or walk-in purchases.
-- These are analyzed separately from branch benchmarks due to their distinct sales nature.


-- (5.4) Service vs Spare parts-Value Density 
-- Which category generates more value per transaction?

SELECT
    Item_Category,
    COUNT(DISTINCT Invoice) AS invoices,
    SUM(Total) AS revenue,
    ROUND(SUM(Total) / COUNT(DISTINCT Invoice), 2) AS revenue_per_invoice
FROM sales_all_branches_final
GROUP BY Item_Category; 

-- Revenue mix efficiency confirmation

-- [INSIGHT]
-- Spare parts drive higher-value invoices (₹1,551 on 8.4K invoices), while services average ₹530 across 8.2K invoices, reflecting a high-frequency, low-ticket model for customer engagement. 
-- The 3× gap highlights spare parts as the financial engine and services as a retention layer. 
-- Strategic opportunities include upselling parts, optimizing service pricing, or bundling offerings to boost overall invoice value.


-- (5.5)High-Value Invoice Identification 
-- Are there premium invoices driving disproportionate revenue?

 SELECT
    Invoice,
    SUM(Total) AS invoice_value
FROM sales_all_branches_final
GROUP BY Invoice
ORDER BY invoice_value DESC
LIMIT 10;

-- This shows ; premium customer behavior,upsell success, revenue concentration risk/opportunity

-- [INSIGHT]
-- Top invoices range ₹19K–₹24K, far above the average ₹1,060, driven by bulk spare-part purchases or bundled services. 
-- Though rare, they contribute disproportionately to revenue.
-- Strategically, identifying their drivers—customer type, vehicle, or item mix—enables targeted upselling, premium offerings, and engagement with high-value clients, boosting revenue efficiency without increasing volume.

-- Step 5 summary ;

-- • The business generated ₹17.42M from 16.4K invoices and 70.8K items, reflecting a high-volume, multi-item sales model. 
-- • Palayam drives scale through volume, while Muttathara delivers higher per-invoice value.
-- • Spare parts generate nearly 3× the revenue per invoice compared to services, which support engagement and retention. Rare high-value invoices (₹19K–₹24K) offer additional upside.
-- • Step 5 highlights where scale, value, and leverage exist, guiding margin, pricing, and customer-level profitability strategies.


-- STEP 6 [CUSTOMER AND DEMAND INTELLIGENCE]

-- • The objective of step 6 is to ;
-- • Whether revenue is healthy and diversified or dangerously concentrated
-- • Who the high-impact customers / invoices are
-- • Whether growth is scalable or dependent on a few entities
-- • Where relationship-based upsell opportunities exist

-- (6.1)  Calculate revenue contribution by each invoice
-- Is most revenue coming from a small fraction of invoices? 

SELECT 
    Invoice,
    SUM(Total) AS invoice_value
FROM spare_parts.sales_all_branches_final
GROUP BY Invoice
ORDER BY invoice_value DESC;

-- This identifies which invoices generate the most revenue,Helps demonstrate revenue concentration and risk exposure.

-- [INSIGHT]
-- A small fraction of high-value invoices (₹20,000+) contributes a disproportionate share of total revenue.
-- While most invoices are lower-value, premium transactions significantly influence overall performance.



-- (6.2) High impact Invoice segmentation
-- which invoices behave like "VIP customers"?

WITH invoice_revenue AS (
    SELECT Invoice, SUM(Total) AS invoice_value
    FROM sales_all_branches_final
    GROUP BY Invoice
)
SELECT
    Invoice,
    invoice_value,
    ROUND(100 * invoice_value / SUM(invoice_value) OVER (), 2) AS revenue_share_pct
FROM invoice_revenue
ORDER BY invoice_value DESC;

-- This shows the proportion of revenue captured by top invoices.
-- Highlights revenue dependency risk and identifies high-impact transactions.

-- [INSIGHT] 
-- Revenue distribution is highly skewed: invoices above ₹10,000 account for a disproportionately large share of total revenue despite representing a small fraction of total transactions.
-- This confirms revenue concentration risk while also highlighting scalable premium-selling behavior that can be strategically expanded.

 
-- (6.3) Repeat vs One Time Transaction Behavior
-- Is the business built on repeat demand or one-off purchases?

SELECT 
    Invoice, COUNT(*) AS transactions_per_invoice
FROM spare_parts.sales_all_branches_final
GROUP BY Invoice
ORDER BY transactions_per_invoice DESC;

-- Determines whether revenue is repeat-driven or single transactions and demonstrates operational and customer loyalty insights.

-- [INSIGHT]
-- Most invoices represent single purchase events, while a smaller set contains many line items,
-- indicating bulk purchases or large repair jobs.
-- This highlights opportunities to convert high-value transactional customers into repeat buyers.

-- This shows the business is transaction-driven, highlighting an opportunity to grow revenue by converting occasional buyers into repeat clients.

-- (6.4) Strategic Reference: Top Revenue-Driving Invoices
-- Used for customer profiling, premium behavior analysis, and retention targeting


SELECT 
    Invoice,
    SUM(Total) AS invoice_value
FROM spare_parts.sales_all_branches_final
GROUP BY Invoice
ORDER BY invoice_value DESC
LIMIT 10;

-- Shows premium transactions driving disproportionate revenue.

-- [INSIGHT]
-- These invoices represent strategic revenue anchors rather than routine sales.
-- Protecting and replicating the behaviors behind these transactions can significantly improve revenue without increasing customer volume.



-- (6.5) RISK AND OPPORTUNITY INTERPRETATION

-- Objective:
-- • The goal of this step is to interpret the business insights from previous analyses (Steps 4 & 5) to identify areas of risk and opportunities for growth, efficiency, and revenue optimization
-- • Unlike prior steps, this is insight-heavy, focusing on strategic evaluation rather than running new queries.

-- • Context:
-- In prior steps, we analyzed revenue trends, branch performance, category contribution, and invoice-level efficiency.
-- This Step consolidates these findings to highlight business risks (such as over-dependence on a single branch or revenue stream) and opportunities (like upselling services, optimizing branch efficiency, or targeting high-value customers).
-- This demonstrates strategic thinking, going beyond numbers to actionable business intelligence.

-- • Risk Interpretation:

-- 1) Branch Dependence Risk:

-- Analysis shows that 3W Spares Counter Palayam contributes more than 50% of total revenue.
-- Risk: Any disruption at Palayam (inventory shortage, operational issues, or customer churn) could significantly impact overall revenue.
-- Helps to understand operational vulnerabilities and risk mitigation.

-- 2) Revenue Concentration Risk:

-- Top 1% of invoices account for disproportionately high revenue.
-- Risk: Business depends heavily on a few high-ticket transactions. If these customers stop buying or services fail, revenue could drop.
-- Highlights customer dependency awareness and focus on stabilizing revenue streams.

-- (3) Service Revenue Underutilization:

-- Services contribute ~25% of revenue despite high transaction volumes.
-- Risk: Low revenue per service transaction indicates potential undervaluation of service work or missed upsell opportunities.

-- • Opportunity Interpretation:

-- (1) Upselling & Bundling:
-- Since customers purchase multiple items per invoice, there’s an opportunity to bundle services with spare parts to increase average invoice value.

-- (2) Branch Optimization & Best Practice Replication:
-- Palayam drives scale, Muttathara drives per-invoice value.
-- Opportunity: Replicate Muttathara’s high-value strategies at Palayam, or scale Palayam’s volume-focused model at smaller branches.

-- (3) Target High-Value Customers:
-- Identifying premium invoices and top customers allows focused engagement.
-- Opportunity: Create loyalty programs, premium services, or proactive marketing to maximize revenue efficiency.

-- (4) Data & Process Improvements:
-- Counter Sales (~₹2M revenue) represent a significant direct sales channel.
-- Improving tagging and tracking of counter transactions can enhance visibility, pricing control, and customer insight.
-- Opportunity: Improve ERP entry, branch tagging, and reporting to enhance accuracy of decision-making.


-- [Insight]
-- Analysis highlights strong revenue but dependence on Palayam, spare parts, and a few high-value invoices, revealing operational and concentration risks.
-- Opportunities include service upselling, branch optimization, premium customer targeting, and better data capture. 
-- This step converts transactional data into actionable intelligence, demonstrating professional-level analysis that drives growth while mitigating risk.



-- STEP 6 summary ;

-- Key Findings;
-- • Revenue is concentrated in a small number of high-value invoices, with top invoices exceeding ₹20K–₹30K.
-- • The business benefits from premium transactions, not just high sales volume.
-- • A single branch (Palayam) contributes a significant share of total revenue, creating operational dependency.
-- • Service transactions are under-monetized despite high usage.

-- Business Implications:
-- • Strong revenue performance, but exposed to concentration risk (top invoices + dominant branch).
-- • Clear opportunity to increase average invoice value through service upselling and bundling.
-- • Replicating best-performing branch practices can improve scalability and resilience.



-- =========================================================
-- STEP 7 : KPI FRAMEWORK & BUSINESS HEALTH METRICS
-- =========================================================

-- OBJECTIVE:
-- To define a structured KPI framework that enables continuous monitoring of revenue health, demand quality, operational efficiency,
-- and risk exposure across branches, invoices, and revenue streams.

-- This step converts analytical findings into measurable performance
-- indicators that support long-term business tracking and decision-making.

-- =========================================================
-- KPI CATEGORY 1 : REVENUE HEALTH METRICS
-- =========================================================

-- Total Revenue (Monthly / Branch-wise)
-- Purpose: Track overall revenue growth and seasonality trends.

-- Revenue Split by Category (Spare Parts vs Service)
-- Purpose: Measure revenue diversification and dependency risk.

-- Average Invoice Value (AIV)
-- Purpose: Evaluate pricing strength and upselling effectiveness.

-- =========================================================
-- KPI CATEGORY 2 : CUSTOMER & DEMAND INTELLIGENCE
-- =========================================================

-- Top 1% Invoice Revenue Share
-- Purpose: Monitor revenue concentration and dependency on high-value invoices.

-- High-Value Invoice Count (Invoices > ₹10,000)
-- Purpose: Track growth of premium transactions.

-- Repeat vs One-Time Transaction Ratio
-- Purpose: Assess demand stability and repeat purchase behavior.

-- =========================================================
-- KPI CATEGORY 3 : OPERATIONAL EFFICIENCY METRICS
-- =========================================================

-- Revenue per Invoice
-- Purpose: Measure transaction quality and sales efficiency.

-- Revenue per Branch
-- Purpose: Compare productivity and performance across branches.

-- High Line-Item Invoice Ratio
-- Purpose: Identify operational complexity and billing workload drivers.

-- =========================================================
-- KPI CATEGORY 4 : RISK MONITORING METRICS
-- =========================================================

-- Branch Dependency Ratio
-- Purpose: Detect over-reliance on a single revenue-generating branch.

-- Service Revenue Share (%)
-- Purpose: Identify under-monetization of service operations.

-- Counter Sale Revenue Percentage
-- Purpose: Monitor contribution and performance of direct walk-in sales


-- =========================================================
-- STEP 7 SUMMARY
-- =========================================================

-- • This KPI framework establishes a structured performance monitoring layer
-- • over revenue, customers, operations, and risk dimensions.
-- • It enables scalable tracking of business health without re-running
-- • deep analytical queries, strengthening long-term visibility and control.





-- =========================================================
-- STEP 8 : DECISION & ACTION BLUEPRINT
-- =========================================================

-- OBJECTIVE:
-- To convert analytical insights and KPIs into clear, executable
-- business actions that improve revenue stability, efficiency,
-- and scalability.

-- This step bridges data analysis with real-world decision-making
-- by defining WHAT actions to take, WHERE to focus, and WHY.

-- =========================================================
-- DECISION AREA 1 : REVENUE STABILITY & RISK CONTROL
-- =========================================================

-- Action 1: Mitigate Revenue Concentration Through Branch Diversification
-- Focus: High revenue concentration at 3W Spares Counter Palayam
-- Decision:
-- - Strengthen inventory depth and service capacity at secondary branches
-- - Gradually distribute high-value transactions across locations
-- Impact:
-- - Lower operational risk
-- - Improved revenue resilience

-- - Success Metrics ;
-- Reduction in revenue contribution share of the dominant branch (Palayam) over time
-- Measurable increase in revenue contribution from secondary branches without loss of total revenue
-- Improved revenue distribution balance across branches, reducing single-point operational risk

-- Action 2: Protect High-Value Invoices
-- Focus: Top 1% invoices contributing disproportionate revenue
-- Decision:
-- - Monitor pricing accuracy for premium invoices
-- - Prioritize fulfillment accuracy and turnaround time
-- Impact:
-- - Revenue protection
-- - Reduced risk from customer churn

-- - Success Metrics ; 
-- Reduction in revenue contribution share of the dominant branch (Palayam) over time
-- Measurable increase in revenue contribution from secondary branches without loss of total revenue
-- Improved revenue distribution balance across branches, reducing single-point operational risk

-- =========================================================
-- DECISION AREA 2 : REVENUE GROWTH LEVERS
-- =========================================================

-- Action 3: Upsell Service with Spare Parts
-- Focus: Low service revenue share despite high transaction volume
-- Decision:
-- - Bundle services with spare-part-heavy invoices
-- - Promote preventive maintenance services
-- Impact:
-- - Increased average invoice value
-- - Better service monetization

-- Success Metrics;
-- - Growth in Average Invoice Value (AIV) without proportional increase in transaction volume
-- Increase in service revenue share as a percentage of total revenue
-- Higher attachment rate of services on spare-part-heavy invoices

-- Action 4: Replicate High-Value Branch Behavior
-- Focus: Muttathara shows higher invoice value efficiency
-- Decision:
-- - Identify pricing, product mix, and service patterns
-- - Replicate successful strategies across other branches
-- Impact:
-- - Scalable revenue growth
-- - Improved branch performance consistency

-- Success Metrics;
-- Reduction in variance of revenue per invoice between branches
-- Observable uplift in invoice value at lower-performing branches after strategy replication
-- Consistent pricing and product mix behavior across branches
-- =========================================================
-- DECISION AREA 3 : OPERATIONAL EFFICIENCY
-- =========================================================

-- Action 5: Optimize High Line-Item Invoices
-- Focus: Invoices with excessive line items
-- Decision:
-- - Review pricing tiers for bulk purchases
-- - Introduce structured billing for large jobs
-- Impact:
-- - Reduced operational load
-- - Improved billing accuracy

-- Success Metrics;
-- Decrease in average line items per invoice for bulk transactions without revenue loss
-- Improved billing turnaround time for complex invoices
-- Reduction in billing corrections or invoice rework for large transactions

-- =========================================================
-- DECISION AREA 4 : DATA & PROCESS IMPROVEMENT
-- =========================================================

-- Action 6: Optimize Counter Sale Tracking
-- Focus: High-value walk-in and bulk counter transactions
-- Decision:
-- - Standardize counter sale tagging
-- - Introduce customer identification where possible
-- Impact:
-- - Better revenue visibility
-- - Improved upselling and customer retention

-- Success Metrics; 
-- Increase in accurately tagged and classified counter sale transactions
-- Growth in counter sale revenue share with improved customer traceability
-- Enhanced ability to identify repeat walk-in customers and premium bulk buyers

-- =========================================================
-- STEP 8 SUMMARY
-- =========================================================

-- • This step defines a clear action roadmap aligned with analytical findings.
-- • It ensures insights are operationalized into decisions that enhance
-- • revenue stability, growth, efficiency, and data integrity.
-- • The blueprint positions the analysis as decision-ready intelligence
-- • rather than static reporting.




-- =========================================================
-- STEP 9 : EXECUTIVE CONCLUSION & PROJECT IMPACT
-- =========================================================

-- OBJECTIVE:
-- To consolidate the full analytical journey into a single,
-- high-level business narrative that demonstrates data maturity,
-- decision readiness, and strategic impact.

-- This step closes the project by clearly answering:
-- - What was analyzed?
-- - What was discovered?
-- - Why it matters?
-- - How this analysis adds business value?

-- =========================================================
-- PROJECT SCOPE SUMMARY
-- =========================================================

-- The analysis examined multi-branch sales data covering:
-- - Revenue performance
-- - Branch efficiency
-- - Category contribution (Spare Parts vs Services)
-- - Invoice-level behavior
-- - Customer demand patterns
-- - Revenue concentration and operational risk

-- The project progressed from raw data exploration
-- to profitability analysis, customer intelligence,
-- and finally decision-driven business strategy.

-- =========================================================
-- KEY BUSINESS FINDINGS
-- =========================================================

-- 1) Revenue Model Structure
-- - Business operates on a high-volume, mid-value billing model
-- - Spare parts act as the primary revenue engine
-- - Services drive customer engagement and repeat visits

-- 2) Revenue Concentration
-- - A small percentage of invoices contribute a
--   disproportionate share of total revenue
-- - One primary branch contributes more than half
--   of overall revenue

-- 3) Operational Efficiency
-- - Customers typically purchase multiple line items per invoice
-- - Certain branches demonstrate higher value efficiency
-- - Large invoices increase revenue but add operational complexity

-- =========================================================
-- STRATEGIC VALUE DELIVERED
-- =========================================================

-- This project delivers:
-- - Clear visibility into revenue stability and risk exposure
-- - Identification of high-impact invoices and demand patterns
-- - Branch-level performance differentiation
-- - Action-ready insights for revenue growth and efficiency
-- - A structured framework for scaling performance
--   without increasing transaction volume

-- =========================================================
-- FINAL OUTCOME
-- =========================================================

-- • The analysis transforms transactional sales data
-- • into executive-level business intelligence.
-- • It demonstrates the ability to:
-- - Ask the right business questions
-- - Use SQL to answer them precisely
-- - Translate numbers into decisions
-- - Design scalable, risk-aware growth strategies

-- • This concludes the end-to-end analytical workflow,
-- • positioning the project as a complete,
-- • decision-oriented data analysis case study.
