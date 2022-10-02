{% macro parquet__create_csv_table(model, agate_table) %}
    -- no-op
{% endmacro %}

{% macro parquet__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {{ adapter.drop_relation(old_relation) }}
{% endmacro %}

{% macro parquet__load_csv_rows(model, agate_table) %}
  {{ adapter.load_dataframe(model['database'], model['schema'], model['alias'],
  							agate_table) }}
{% endmacro %}
