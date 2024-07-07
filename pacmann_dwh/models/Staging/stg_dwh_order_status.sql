{{ config(
    schema='staging',
    alias='stg_order_status'
    ) 
}}

SELECT *
FROM {{ source('pacbook', 'order_status') }}