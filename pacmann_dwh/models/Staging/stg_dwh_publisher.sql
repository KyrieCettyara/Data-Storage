{{ config(
    schema='staging',
    alias='stg_publisher'
    ) 
}}

SELECT *
FROM {{ source('pacbook', 'publisher') }}