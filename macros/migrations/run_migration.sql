{% macro run_migrations(
            database = target.database,
            schema_prefix = target.schema
) -%}

{#% do log(
    "Running V001_drop_example_table migration with database = " ~ database ~ ", schema_prefix = " ~ schema_prefix,
    info=True) %#}

{#% do run_query(V001_drop_example_table(
                        database, schema_prefix)) %#}    

{% do log(
    "Running V003_drop_table migration with database = " ~ database ~ ", schema_prefix = " ~ schema_prefix,
    info=True) %}

{% do run_query(V003_drop_table(
                        database, schema_prefix)) %}    

{#% do log("No migration to run.", info=True) %#}                        
-- Remove # to uncomment if no migration to run

{%- endmacro %}