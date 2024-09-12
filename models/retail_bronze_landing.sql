{{ config(materialized='view') }}

select
    Invoice as Invoice_Number,
    StockCode,
    Description,
    Quantity,
    InvoiceDate,
    Price,
    `Customer ID` as Customer_ID,
    Country
from {{ source('default', 'online_retail') }}