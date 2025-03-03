with source as (
    select * from {{ source('raw_airbnb', 'raw_listings') }}
)

