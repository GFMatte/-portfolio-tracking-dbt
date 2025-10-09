{{ config(materialized='ephemeral') }}

WITH src_data AS (
    SELECT
         ALPHABETIC_CODE                    AS ALPHABETIC_CODE             --TEXT
        ,NUMERIC_CODE                       AS NUMERIC_CODE                --NUMERIC
        ,DECIMAL_DIGITS                     AS DECIMAL_DIGITS              --NUMERIC
        ,CURRENCY_NAME                      AS CURRENCY_NAME               --NUMERIC
        ,LOCATIONS                          AS LOCATIONS_NAME              --TEXT
        ,LOAD_TS                            AS LOAD_TS                     --TIMESTAMP_NTZ
        ,'SEED.ABC_BANK_CURRENCY_INFO'      AS RECORD_SOURCE               --TEXT

    FROM {{ source("seeds", 'ABC_BANK_CURRENCY_INFO') }}
),
default_record AS (
    SELECT
         'Missing'                          AS ALPHABETIC_CODE
        ,'-1     '                          AS NUMERIC_CODE
        ,'-1     '                          AS DECIMAL_DIGITS
        ,'Missing'                          AS CURRENCY_NAME
        ,'Missing'                          AS LOCATIONS_NAME             
        ,'1900-01-01'                       AS LOAD_TS
        ,'System.DefaultKey'                AS RECORD_SOURCE 
),
with_default_record AS (
    SELECT * FROM src_data
    UNION ALL
    SELECT * FROM default_record
),
hashed AS (
    SELECT
         concat_ws('|', ALPHABETIC_CODE)         AS CURRENCY_HKEY
        ,concat_ws('|',ALPHABETIC_CODE,NUMERIC_CODE,
        DECIMAL_DIGITS, CURRENCY_NAME,
        LOCATIONS_NAME)                          AS CURRENCY_HDIFF
        , * EXCLUDE LOAD_TS                    
        , LOAD_TS                                AS LOAD_TS_UTC
    
    FROM with_default_record
)

SELECT * FROM hashed
