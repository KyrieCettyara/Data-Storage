{{ config(
    schema='staging',
    alias='stg_author'
    ) 
}}

SELECT *
FROM {{ source('pacbook', 'author') }}