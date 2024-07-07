{{ config(
    schema='staging',
    alias='stg_address'
    ) 
}}

SELECT *
FROM {{ source('pacbook', 'address') }}