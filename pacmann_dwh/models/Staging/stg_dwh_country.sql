{{ config(
    schema='staging',
    alias='stg_country'
    ) 
}}

SELECT *
FROM {{ source('pacbook', 'country') }}