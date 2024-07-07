{{ config(
    schema='staging',
    alias='stg_cust_order'
    ) 
}}

SELECT *
FROM {{ source('pacbook', 'cust_order') }}