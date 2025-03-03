with guest_bookings as (
    select
        reviewer_id,
        reviewer_name,
        listing_id,
        count(review_id) as review_count
    from {{ ref('stg_raw_airbnb_data__reviews') }}
    group by reviewer_id, reviewer_name, listing_id
),
repeat_visitors as (
    select
        reviewer_id,
        reviewer_name,
        listing_id,
        review_count,
        case
            when review_count > 1 then 'repeat visitor'
            else 'one-time visitor'
        end as visit_type
    from guest_bookings
)

select * from repeat_visitors
