{{ config(
    schema='staging',
    alias='stg_customer_address'
    ) 
}}

SELECT *
FROM {{ source('pacbook', 'customer_address') }}