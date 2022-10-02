{% macro parquet__get_catalog(information_schema, schemas) -%}

  {%- call statement('catalog', fetch_result=True) -%}

    with parquet_cols as (
      select
        string_split(file_name, '/') as file_parts,
        len(string_split(file_name, '/')) as num_parts,
        path_in_schema as column_name,
        column_id as column_index,
        "type" as column_type
      from parquet_metadata('{{ information_schema.path.database }}/**/*.parquet')
    )

    select
        '{{ information_schema.path.database }}' as table_database,
        file_parts[num_parts - 1] as table_schema,
        string_split(file_parts[num_parts], '.parquet')[1] as table_name,
        'table' as table_type,
        '' as table_comment,
        column_name,
        column_index,
        column_type,
        '' as column_comment,
        '' as table_owner
    FROM parquet_cols
    WHERE table_schema IN ('{{ schemas | join("', '") }}')
    ORDER BY
        table_schema,
        table_name,
        column_name

  {%- endcall -%}
  {{ return(load_result('catalog').table) }}

 {% endmacro %}
