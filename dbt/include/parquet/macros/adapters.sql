/* For examples of how to fill out the macros please refer to the postgres adapter and docs
postgres adapter macros: https://github.com/dbt-labs/dbt-core/blob/main/plugins/postgres/dbt/include/postgres/macros/adapters.sql
dbt docs: https://docs.getdbt.com/docs/contributing/building-a-new-adapter
*/

{% macro parquet__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {{ sql_header if sql_header is not none }}

  copy ({{ sql }}) to '{{ relation.render_path() }}' (format 'parquet');
  {{ relation.register_as_view_cmd() }};
{%- endmacro %}

{% macro parquet__create_view_as(relation, sql) -%}
  -- For a parquet file, View == Table.
  {%- set sql_header = config.get('sql_header', none) -%}
  {{ sql_header if sql_header is not none }}

  copy ({{ sql }}) to '{{ relation.render_path() }}' (format 'parquet');
  {{ relation.register_as_view_cmd() }};
{%- endmacro %}

{% macro parquet__snapshot_string_as_time(timestamp) -%}
    {%- set result = "'" ~ timestamp ~ "'::timestamp" -%}
    {{ return(result) }}
{%- endmacro %}

{% macro parquet__snapshot_get_time() -%}
  {{ current_timestamp() }}::timestamp
{%- endmacro %}

{% macro parquet__current_timestamp() -%}
'''Returns current UTC time'''
now()
{% endmacro %}
