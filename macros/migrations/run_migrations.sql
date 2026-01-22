{% macro run_migrations(
        database = target.database,
        schema_prefix = target.schema
) -%}

{% do run_migration('V003_drop_table', database, schema_prefix) %}

{%- endmacro %}