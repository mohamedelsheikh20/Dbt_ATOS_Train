with

source as (

    select
        
        *
        
    from {{ ref('stg_schema_superstore__superstore_sample') }}
),

dim_customer_cte as (

    select 

        *
    
    from {{ ref('dim_customer') }}

),


dim_location_cte as (

    select 

        *
    
    from {{ ref('dim_location') }}

),

dim_product_cte as (

    select 

        *
    
    from {{ ref('dim_product') }}

),


dim_calendar_date_cte as (

    select 

        *
    
    from {{ ref('dim_calendar_date') }}

),


fact_orders as (

    select

        -- fact table key
        row_id,

        -- foriegn keys using join
        dim_customer_cte.customer_id,
        dim_product_cte.product_id,
        dim_location_cte.location_id,

        -- dates foriegn keys 
        dim_calendar_date_order.date_id as order_date_id,
        dim_calendar_date_ship.date_id as ship_date_id,

        -- degenerated dimension
        ship_mode as dd_ship_mode,

        -- measuers columns
        sales,
        quantity,
        discount,
        profit
    
    from source


    -- join customer dimension
    left join dim_customer_cte on
        dim_customer_cte.customer_id = source.customer_id


    -- join product dimension
    left join dim_product_cte on
        dim_product_cte.product_id = source.product_id


    -- join order date in calendar dimension
    left join dim_calendar_date_cte as dim_calendar_date_order on
        dim_calendar_date_order.main_date = source.order_date


    -- join ship date in calendar dimension
    left join dim_calendar_date_cte as dim_calendar_date_ship on
        dim_calendar_date_ship.main_date = source.ship_date


    -- join location dimension
    left join dim_location_cte on
        source.country = dim_location_cte.location_country and
        source.city = dim_location_cte.location_city and
        source.state = dim_location_cte.location_state and
        source.postal_code = dim_location_cte.location_postal_code and
        source.region = dim_location_cte.location_region

)

select * from fact_orders
