-- First read all data from the HIST table
WITH
position_history as (
    SELECT *
    FROM {{ ref('HIST_ABC_BANK_POSITION_WITH_CLOSING') }}
)

-- Then brings previous transactions data with LAG() function
, position_with_lead as (
    SELECT 
          p.*
        , LAG(QUANTITY)  OVER(PARTITION BY POSITION_HKEY ORDER BY LOAD_TS_UTC) AS PREV_QUANTITY
        , LAG(COST_BASE) OVER(PARTITION BY POSITION_HKEY ORDER BY LOAD_TS_UTC) AS PREV_COST_BASE
        , LAG(POSITION_VALUE) OVER(PARTITION BY POSITION_HKEY ORDER BY LOAD_TS_UTC) AS PREV_POSITION_VALUE
    FROM position_history as p 
)

, tx_calculation as (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(
            ['ACCOUNT_CODE', 'SECURITY_CODE', 'REPORT_DATE']
        ) }} as TX_HKEY
        , ACCOUNT_CODE
        , SECURITY_CODE
        , REPORT_DATE
        , SECURITY_NAME
        , EXCHANGE_CODE
        , CURRENCY_CODE
        , RECORD_SOURCE
        , LOAD_TS_UTC
        , COALESCE(QUANTITY - PREV_QUANTITY, QUANTITY) as QUANTITY_PURCHASED
        , CASE
            WHEN QUANTITY_PURCHASED > 0 THEN 'BUY'
            WHEN QUANTITY_PURCHASED < 0 THEN 'SELL'
            ELSE 'UNCHANGED'
          END as TX_TYPE
        , CASE
            WHEN QUANTITY_PURCHASED > 0
              THEN -1 * COALESCE(COST_BASE - PREV_COST_BASE, COST_BASE)
            WHEN QUANTITY_PURCHASED < 0
              THEN IFF(QUANTITY = 0, PREV_POSITION_VALUE, 
                       POSITION_VALUE / QUANTITY * QUANTITY_PURCHASED)
            ELSE 0
          END as ESTIMATED_TX_PROCEEDS
        , QUANTITY       as FINAL_QUANTITY
        , COST_BASE      as FINAL_COST_BASE
        , POSITION_VALUE as FINAL_POSITION_VALUE
    FROM position_with_lead as p
)           

SELECT * FROM tx_calculation
ORDER BY ACCOUNT_CODE, SECURITY_CODE, REPORT_DATE