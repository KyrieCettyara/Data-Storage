{{ config(
    schema='staging',
    alias='stg_customer'
    ) 
}}

SELECT *
FROM {{ source('pacbook', 'customer') }}