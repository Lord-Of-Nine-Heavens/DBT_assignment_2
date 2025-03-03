{{ config(
    materialized='incremental',
    unique_key='listing_id',
    cluster_by=['listing_id'],
    incremental_strategy='merge'
) }}

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
        CASE
            WHEN minimum_nights = 0 THEN 1
            ELSE minimum_nights
        END AS minimum_nights,
        host_id,
        REPLACE(REPLACE(price, '$', ''), ',', '')::NUMBER(10,2) AS price_final,
        created_at,
        updated_at
    from source
    where
    price_final > 0
    and
    minimum_nights > 0 
    and
    {%- for column in columns %}
    {{ column }} is not null
    {% if not loop.last -%}
        and
    {%- endif -%}
    {%- endfor -%}
    {% if is_incremental() %}
    and updated_at > (SELECT MAX(updated_at) FROM {{ this }})
    {% endif %}

)

select * from transformed

