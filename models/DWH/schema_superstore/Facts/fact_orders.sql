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


fact_orders as (

    select

        -- fact table key
        row_id,

        -- foriegn keys using join
        dim_customer_cte.customer_id,
        dim_product_cte.product_id,
        dim_location_cte.location_id,

        -- generate dates foriegn keys 
        to_number(to_varchar(order_date, 'YYYYMMDD')) as date_id_order,
        to_number(to_varchar(ship_date, 'YYYYMMDD')) as date_id_ship,

        -- degenerated dimension
        ship_mode as dd_ship_mode,

        -- measuers columns
        sales,
        quantity,
        discount,
        profit
    
    from source

    left join dim_customer_cte on
        dim_customer_cte.customer_id = source.customer_id

    left join dim_product_cte on
        dim_product_cte.product_id = source.product_id

    left join dim_location_cte on
        source.country = dim_location_cte.location_country and
        source.city = dim_location_cte.location_city and
        source.state = dim_location_cte.location_state and
        source.postal_code = dim_location_cte.location_postal_code and
        source.region = dim_location_cte.location_region

)

select * from fact_orders
