-- Query to get top city and states who make the profit be negative.

with

source as (

    select

        *
    
    from {{ ref('fact_orders') }}

),

location_dim as (

    select

        *
    
    from {{ ref('dim_location') }}

),

negative_profit_per_location as (

    select

        location_id,
        sum(profit) as profit_sum
        
    from source
    group by location_id
    having profit_sum < 0
    
),

negative_profit_city_state as (

    select

        location_dim.location_city,
        location_dim.location_state,
        negative_profit_per_location.profit_sum as negative_profit_sum
        
    from negative_profit_per_location

    left join location_dim on
        location_dim.location_id = negative_profit_per_location.location_id

    order by profit_sum
    
)

select * from negative_profit_city_state

