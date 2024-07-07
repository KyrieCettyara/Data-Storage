{{ config(
    schema='staging',
    alias='stg_order_line'
    ) 
}}

SELECT *
FROM {{ source('pacbook', 'order_line') }}