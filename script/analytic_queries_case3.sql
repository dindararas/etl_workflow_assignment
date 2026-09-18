-- QUERY 1 :
-- Top 10 most valuable customers (champions)
SELECT
    "CustomerName"
    , "TerritoryName"
    , "Recency"
    , "Frequency"
    , "Monetary"
    , "RFM_Score"
    , "Segment"
FROM "dwh"."fact_customer_rfm"
WHERE "Segment" = 'Champions'
ORDER BY "RFM_Score" DESC, "Monetary" DESC
LIMIT 10;

-- QUERY 2 : Distribution of customer segment
SELECT
	"Segment"
	, COUNT(DISTINCT "CustomerID") AS "Number of Customers"
	, ROUND(COUNT(DISTINCT "CustomerID") * 100.0 / (SELECT COUNT(*) FROM "dwh"."fact_customer_rfm"),2) as "percentage"
FROM "dwh"."fact_customer_rfm"
GROUP BY 1;

-- QUERY 3 : profiles of each customer segment
SELECT
	"Segment"
	, MAX("Recency") as "max_recency"
	, ROUND(AVG("Recency"),2) as "avg_recency"
	, MIN("Recency") as "min_recency"
	, MAX("Frequency") as "max_frequency"
	, ROUND(AVG("Frequency"),2) as "avg_frequency"
	, MIN("Frequency") as "min_frequency"
	, MAX("Monetary") as "max_monetary"
	, ROUND(AVG("Monetary"),2) as "avg_monetary"
	, MIN("Monetary") as "min_monetary"
FROM "dwh"."fact_customer_rfm"
GROUP BY 1;

-- QUERY 4 : customers with recency >365 and high monetary
SELECT
    "CustomerName"
    , "TerritoryName"
    , "Recency"
    , "Frequency"
    , "Monetary"
FROM "dwh"."fact_customer_rfm"
WHERE "Recency" > 365
ORDER BY 5 desc
LIMIT 10;

-- QUERY 5: Avg RFM score per region
SELECT
    "TerritoryName"
	, ROUND(AVG("RFM_Score"),2) as "avg_rfm" 
FROM "dwh"."fact_customer_rfm"
GROUP BY 1;

SELECT *
FROM "dwh"."fact_customer_rfm"
LIMIT 10;
