with nightly_rates as (
    select 
        listing_id, 
        price_final as nightly_rate
    from {{ ref('stg_raw_airbnb_data__listings') }}
),
average_nightly_rate as (
select 
    listing_id,
    AVG(nightly_rate) as avg_nightly_rate
from nightly_rates
group by listing_id
)

select * from average_nightly_rate
