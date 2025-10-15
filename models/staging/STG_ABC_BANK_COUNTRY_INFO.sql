{{ config(materialized='ephemeral') }}

WITH src_data AS (
    SELECT
         COUNTRY_NAME                       AS COUNTRY_NAME                --TEXT
        ,COUNTRY_CODE_2_LETTER              AS COUNTRY_CODE                --TEXT
        ,COUNTRY_CODE_3_LETTER              AS COUNTRY_CODE_3_LETTER       --TEXT
        ,COUNTRY_CODE_NUMERIC               AS COUNTRY_CODE_NUMERIC        --NUMERIC
        ,ISO_3166_2                         AS ISO_3166_2_CODE             --TEXT
        ,REGION                             AS REGION_NAME                 --TEXT
        ,SUB_REGION                         AS SUB_REGION_NAME             --TEXT
        ,INTERMEDIATE_REGION                AS INTERMEDIATE_REGION_NAME    --TEXT
        ,REGION_CODE                        AS REGION_CODE                 --NUMERIC
        ,SUB_REGION_CODE                    AS SUB_REGION_CODE             --NUMERIC
        ,INTERMEDIATE_REGION_CODE           AS INTERMEDIATE_REGION_CODE    --NUMERIC
        ,LOAD_TS                            AS LOAD_TS                     --TIMESTAMP_NTZ
        ,'SEED.ABC_BANK_COUNTRY_INFO'       AS RECORD_SOURCE               --TEXT

    FROM {{ source("seeds", 'ABC_BANK_COUNTRY_INFO') }}
),
default_record AS (
    SELECT
         'Missing'                          AS COUNTRY_NAME
        ,'Missing'                          AS COUNTRY_CODE
        ,'Missing'                          AS COUNTRY_CODE_3_LETTER
        ,'-1'                               AS COUNTRY_CODE_NUMERIC
        ,'Missing'                          AS ISO_3166_2_CODE             
        ,'Missing'                          AS REGION_NAME
        ,'Missing'                          AS SUB_REGION_NAME
        ,'Missing'                          AS INTERMEDIATE_REGION_NAME
        ,'-1'                               AS REGION_CODE
        ,'-1'                               AS SUB_REGION_CODE
        ,'-1'                               AS INTERMEDIATE_REGION_CODE
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
      {{ dbt_utils.generate_surrogate_key(
            ['COUNTRY_CODE']
      ) }}                                                              AS POSITION_HKEY
      , {{ dbt_utils.generate_surrogate_key(
            ['COUNTRY_NAME', 'COUNTRY_CODE', 'COUNTRY_CODE_3_LETTER', 'COUNTRY_CODE_NUMERIC', 'ISO_3166_2_CODE', 'REGION_NAME', 
             'SUB_REGION_NAME', 'INTERMEDIATE_REGION_NAME', 'REGION_CODE', 'SUB_REGION_CODE', 'INTERMEDIATE_REGION_CODE']
      ) }}                                                              AS POSITION_HDIFF
    ,* EXCLUDE LOAD_TS,
    , LOAD_TS AS LOAD_TS_UTC
    
    FROM with_default_record
)

SELECT * FROM hashed
