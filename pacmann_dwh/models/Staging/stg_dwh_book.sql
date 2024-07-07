{{ config(
    schema='staging',
    alias='stg_book'
    ) 
}}

SELECT *
FROM {{ source('pacbook', 'book') }}