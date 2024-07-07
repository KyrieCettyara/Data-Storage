{{ config(
    schema='staging',
    alias='stg_address_status'
    ) 
}}

SELECT *
FROM {{ source('pacbook', 'address_status') }}