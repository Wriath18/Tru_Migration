{{ config(materialized='table') }}

with source_data as (
    SELECT * FROM {{ ref('retail_bronze_landing') }}
),

cleaned_data AS (
    SELECT
        Invoice_Number,
        StockCode,
        Description,
        CAST(Quantity AS INT) AS Quantity,
        TO_TIMESTAMP(InvoiceDate, 'dd-MM-yyyy HH:mm') AS InvoiceDate,
        CAST(Price AS DECIMAL(10, 2)) AS Price,
        CAST(Customer_ID AS INT) AS Customer_ID,
        TRIM(Country) AS Country
    from source_data
    WHERE Invoice_Number IS NOT NULL
        AND Quantity > 0
        AND PRICE > 0
),

enriched_data AS (
    SELECT
        *,
        CAST(Quantity * Price AS DECIMAL(10, 2)) AS TotalAmount,
        DATE(InvoiceDate) AS InvoiceDay,
        YEAR(InvoiceDate) AS InvoiceYear,
        MONTH(InvoiceDate) AS InvoiceMonth
    FROM cleaned_data
)

SELECT * FROM enriched_data