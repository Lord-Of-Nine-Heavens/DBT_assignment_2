{%- set table_name = source('raw_airbnb', 'raw_hosts').identifier.split('.')[-1] -%}
{%- set columns = get_column_names(table_name) -%}

with source as (
    select * from {{ source('raw_airbnb', 'raw_hosts') }}
),
transformed as (
    select
        id as host_id,
        name as host_name,
        is_superhost,
        created_at,
        updated_at
    from source
    where
    {%- for column in columns %}
    {{ column }} is not null
    {% if not loop.last -%}
        and
    {%- endif -%}
    {%- endfor -%}
)

select * from transformed
