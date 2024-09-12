{{ config(materialized='table') }}


WITH silver_data AS (
    SELECT * FROM {{ ref('retail_cleaned_s') }}
)

SELECT
    InvoiceDay,
    COUNT(DISTINCT Invoice_Number) AS NumberOfInvoices,
    COUNT(DISTINCT Customer_ID) AS NumberOfCustomers,
    SUM(Quantity) AS TotalQuantity,
    SUM(TotalAmount) AS TotalRevenue,
    AVG(TotalAmount) AS AverageOrderValue,
    COUNT(DISTINCT StockCode) AS NumberOfUniqueItems
FROM silver_data
GROUP BY InvoiceDay
ORDER BY InvoiceDay