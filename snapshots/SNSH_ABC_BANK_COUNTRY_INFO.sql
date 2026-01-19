{% snapshot SNSH_ABC_BANK_COUNTRY_INFO %}

{{
  config(
    unique_key='COUNTRY_HKEY',
    strategy='check',
    check_cols=['COUNTRY_HDIFF'],
    enabled=false
  )
}}

SELECT * FROM {{ ref('STG_ABC_BANK_COUNTRY_INFO') }}

{% endsnapshot %}
