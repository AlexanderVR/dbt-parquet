/* For examples of how to fill out the macros please refer to the postgres adapter and docs
postgres adapter macros: https://github.com/dbt-labs/dbt-core/blob/main/plugins/postgres/dbt/include/postgres/macros/adapters.sql
dbt docs: https://docs.getdbt.com/docs/contributing/building-a-new-adapter
*/

{% macro parquet__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {{ sql_header if sql_header is not none }}

  {% for node in model['depends_on']['nodes'] %}
    {% set source = graph['sources'].get(node) or graph['nodes'][node] %}

    {% if source %}

      {% set src_rel = relation.create(
          database=source['database'],
          schema=source['schema'],
          identifier=source['identifier']
      ) %}
      {% do log(src_rel, info=true) %}
      create or replace view {{ src_rel.render() }} as
        select * from {{ src_rel.render_parquet_scan() }};
    {% endif %}
  {% endfor %}
  copy ({{ sql }}) to '{{ relation.render_path() }}' (format 'parquet')
  ;
{%- endmacro %}


{% macro parquet__create_view_as(relation, sql) -%}
  -- For a parquet file, View == Table.
  {%- set sql_header = config.get('sql_header', none) -%}
  {{ sql_header if sql_header is not none }}

  {% for node in model['depends_on']['nodes'] %}
    {% set source = graph['sources'].get(node) or graph['nodes'][node] %}

    {% if source %}

      {% set src_rel = relation.create(
          database=source['database'],
          schema=source['schema'],
          identifier=source['identifier']
      ) %}
      {% do log(src_rel, info=true) %}
      create or replace view {{ src_rel.render() }} as
        select * from {{ src_rel.render_parquet_scan() }};
    {% endif %}
  {% endfor %}
  copy ({{ sql }}) to '{{ relation.render_path() }}' (format 'parquet')
  ;
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
