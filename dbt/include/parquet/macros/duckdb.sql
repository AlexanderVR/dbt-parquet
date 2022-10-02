{% macro duckdb() -%}
  {{ return(adapter.dispatch('duckdb')()) }}
{%- endmacro %}

{% macro parquet__duckdb() -%}
   {{ return(adapter.duckdb()) }}
{%- endmacro %}
