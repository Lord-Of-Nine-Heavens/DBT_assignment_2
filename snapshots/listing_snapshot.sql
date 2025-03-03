{% snapshot listing_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='listing_id',
    strategy='timestamp',
    updated_at='updated_at'
) }}

SELECT 
    listing_id,
    listing_url,
    listing_name,
    room_type,
    minimum_nights,
    host_id,
    price_final,
    created_at,
    updated_at
FROM {{ ref('stg_raw_airbnb_data__listings') }}

{% endsnapshot %}
