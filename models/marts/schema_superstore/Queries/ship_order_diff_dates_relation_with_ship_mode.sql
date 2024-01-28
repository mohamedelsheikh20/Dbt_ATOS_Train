-- Query to get the realtion between ship mode and min-max days of shipping.
-- I let all measures so I can get any relationship I want with measures.

with

source as (

    select

        *
    
    from {{ ref('fact_orders') }}

),

calendar_date_dim as (

    select

        *
    
    from {{ ref('dim_calendar_date') }}

),


date_diff_sale_ship_dates as (

    select 
    
        -- IDs
        ship_date_id,
        order_date_id,

        -- the DD field
        dd_ship_mode,
        
        -- here I let the measures, so it can make other relations with date difference
        sales,
        quantity,
        discount,
        profit,
        
        -- dcd1.main_date as ship_date,
        -- dcd2.main_date as order_date,
        dcd1.main_date - dcd2.main_date as diff_in_days
        
        
    from source
    
    left join calendar_date_dim as dcd1 on
        dcd1.date_id = source.ship_date_id
    
    
    left join calendar_date_dim as dcd2 on
        dcd2.date_id = source.order_date_id
    
),

diff_days_relation_with_ship_mode as (

    select
    

        dd_ship_mode,        
        min(diff_in_days) as min_diff_in_days,
        max(diff_in_days) as max_diff_in_days
    
    from date_diff_sale_ship_dates
    group by dd_ship_mode

)

select * from diff_days_relation_with_ship_mode
