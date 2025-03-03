{%- set table_name = source('raw_airbnb', 'raw_listings').identifier.split('.')[-1] -%}
{%- set columns = get_column_names(table_name) -%}

with source as (
    select * from {{ source('raw_airbnb', 'raw_listings') }}
),
transformed as (
    select
        id as listing_id,
        listing_url,
        name as listing_name,
        room_type,
        minimum_nights,
        host_id,
        price,
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

