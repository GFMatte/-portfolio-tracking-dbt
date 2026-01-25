{{ config(materialized='incremental') }}

-- Reorder the columns by excluding from the load and re-adding at the end.
WITH
stg_input as (
    SELECT
          i.* EXCLUDE (REPORT_DATE, QUANTITY, COST_BASE, POSITION_VALUE, LOAD_TS_UTC)
        , REPORT_DATE
        , QUANTITY
        , COST_BASE
        , POSITION_VALUE
        , LOAD_TS_UTC
        , false as CLOSED
    FROM {{ ref('STG_ABC_BANK_POSITION') }} as i
)


{% if is_incremental() %} -- in incremental runs, do this....

-- Read the current values from the HIST table with the "current_from_history" macro
, current_from_history as (
    {{  current_from_history(
        history_rel = this,
        key_column = 'POSITION_HKEY',
    ) }}
)

-- Select the data to be loaded into the HIST from the input
,load_from_input as (
    SELECT i.*
    FROM stg_input as i
    LEFT OUTER JOIN current_from_history as c
      ON (not c.CLOSED
            and i.POSITION_HDIFF = c.POSITION_HDIFF)
    WHERE c.POSITION_HDIFF is null
)

--Select deleted data (rows in the HIST that are not in the input)
,closed_from_hist as (
    SELECT 
          c.* EXCLUDE (REPORT_DATE, QUANTITY, COST_BASE, POSITION_VALUE
                    ,LOAD_TS_UTC, CLOSED)
        , (SELECT MAX(REPORT_DATE) FROM stg_input) as REPORT_DATE
        , 0 as QUANTITY
        , 0 as COST_BASE
        , 0 as POSITION_VALUE
        , '{{ run_started_at }}' as LOAD_TS_UTC
        , true as CLOSED
    FROM current_from_history as c
    LEFT OUTER JOIN stg_input as i
      ON (i.POSITION_HKEY = c.POSITION_HKEY)
    WHERE not c.CLOSED and i.POSITION_HKEY is null
)

--Build the changes to be stored incrementally by putting the above tables together
, changes_to_store as (
    SELECT * FROM load_from_input
    UNION ALL
    SELECT * FROM closed_from_hist
)

{%- else %} -- if not an incremental run
, changes_to_store as (
    SELECT * FROM stg_input
)

{%- endif %}
SELECT * FROM changes_to_store