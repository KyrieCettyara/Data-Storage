{{ config(
    schema='staging',
    alias='stg_order_history'
    ) 
}}

SELECT *
FROM {{ source('pacbook', 'order_history') }}