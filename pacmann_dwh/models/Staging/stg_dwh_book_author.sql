{{ config(
    schema='staging',
    alias='stg_book_author'
    ) 
}}

SELECT *
FROM {{ source('pacbook', 'book_author') }}