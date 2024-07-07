{{ config(
    schema='staging',
    alias='stg_book_language'
    ) 
}}

SELECT *
FROM {{ source('pacbook', 'book_language') }}