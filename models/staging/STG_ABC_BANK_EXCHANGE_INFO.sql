{{ config(materialized='ephemeral') }}

WITH src_data AS (
    SELECT
         NAME                               AS EXCHANGE_NAME              --TEXT
        ,ID                                 AS EXCHANGE_ID                --TEXT
        ,COUNTRY                            AS COUNTRY_NAME               --TEXT
        ,CITY                               AS CITY_NAME                  --TEXT
        ,ZONE                               AS ZONE                       --TEXT
        ,DELTA                              AS DELTA                      --FLOAT       
        ,DST_PERIOD                         AS DST_PERIOD                 --TEXT         
        ,OPEN                               AS OPEN                       --TIME 
        ,CLOSE                              AS CLOSE                      --TIME   
        ,LUNCH                              AS LUNCH                      --TEXT      
        ,OPEN_UTC                           AS OPEN_UTC                   --TIME      
        ,CLOSE_UTC                          AS CLOSE_UTC                  --TIME      
        ,LUNCH_UTC                          AS LUNCH_UTC                  --TEXT      
        ,LOAD_TS                            AS LOAD_TS                    --TIMESTAMP_NTZ
        ,'SEED.ABC_BANK_EXCHANGE_INFO'      AS RECORD_SOURCE              --TEXT

    FROM {{ source("seeds", 'ABC_BANK_EXCHANGE_INFO') }}
),
default_record AS (
    SELECT
         'Missing'::STRING                  AS EXCHANGE_NAME
        ,'-1'::STRING                       AS EXCHANGE_ID
        ,'Missing'::STRING                  AS COUNTRY_NAME
        ,'Missing'::STRING                  AS CITY_NAME
        ,'Missing'::STRING                  AS ZONE
        ,(-1.0)::FLOAT                      AS DELTA
        ,'Missing'::STRING                  AS DST_PERIOD
        ,TO_TIME('00:00')                   AS OPEN
        ,TO_TIME('00:00')                   AS CLOSE               
        ,'Missing'::STRING                  AS LUNCH               
        ,TO_TIME('00:00')                   AS OPEN_UTC             
        ,TO_TIME('00:00')                   AS CLOSE_UTC              
        ,'Missing'::STRING                  AS LUNCH_UTC              
        ,TO_TIMESTAMP_NTZ('1900-01-01')     AS LOAD_TS                
        ,'System.DefaultKey'::STRING        AS RECORD_SOURCE          
),
with_default_record AS (
    SELECT * FROM src_data
    UNION ALL
    SELECT * FROM default_record
),
hashed AS (
    SELECT
         concat_ws('|', EXCHANGE_ID)             AS EXCHANGE_HKEY
        ,concat_ws('|',
            NVL(TO_VARCHAR(EXCHANGE_NAME), ''),
            NVL(TO_VARCHAR(EXCHANGE_ID), ''),            
            NVL(TO_VARCHAR(COUNTRY_NAME), ''),
            NVL(TO_VARCHAR(CITY_NAME), ''),
            NVL(TO_VARCHAR(ZONE), ''),
            NVL(TO_VARCHAR(DELTA), ''),
            NVL(TO_VARCHAR(DST_PERIOD), ''),
            NVL(TO_CHAR(OPEN, 'HH24:MI:SS'), ''),
            NVL(TO_CHAR(CLOSE, 'HH24:MI:SS'), ''),
            NVL(TO_VARCHAR(LUNCH), ''),
            NVL(TO_CHAR(OPEN_UTC, 'HH24:MI:SS'), ''),
            NVL(TO_CHAR(CLOSE_UTC, 'HH24:MI:SS'), ''),
            NVL(TO_VARCHAR(LUNCH_UTC), '')
        )                                        AS EXCHANGE_HDIFF                     
        , * EXCLUDE LOAD_TS                    
        , LOAD_TS                                AS LOAD_TS_UTC
    
    FROM with_default_record
)

SELECT * FROM hashed
