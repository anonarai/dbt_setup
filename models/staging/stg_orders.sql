with

source as (

    select * from {{ source('ecom', 'raw_orders') }}

    -- data runs to 2026, truncate timespan to desired range,
    -- current time as default
    where CAST(ordered_at AS timestamp) <= CAST(now() AS timestamp)

),

renamed as (

    select

        ----------  ids
        id as order_id,
        store_id as location_id,
        customer as customer_id,

        ---------- properties
        (order_total / 100.0)::float as order_total,
        (tax_paid / 100.0)::float as tax_paid,

        ---------- timestamps
        ordered_at

    from source

)

select * from renamed
