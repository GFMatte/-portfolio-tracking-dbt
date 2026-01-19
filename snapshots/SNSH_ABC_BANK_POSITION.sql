{% snapshot SNSH_ABC_BANK_POSITION %}

{{
    config(
        unique_key= 'POSITION_HKEY',
        strategy= 'check',
        check_cols= ['POSITION_HDIFF'],
        invalidate_hard_deletes= True,
        enabled=false
    )
}}

SELECT * FROM {{ ref('STG_ABC_BANK_POSITION') }}

{% endsnapshot %}