{%- set table_name = source('raw_airbnb', 'raw_reviews').identifier.split('.')[-1] -%}
{%- set columns = get_column_names(table_name) -%}

with source as (
    select * from {{ source('raw_airbnb', 'raw_reviews') }}
),
transformed as (
    select
        id as review_id,
        listing_id,
        date as review_date,
        reviewer_id,
        reviewer_name
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
