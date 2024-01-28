-- top sold product per years

with

source as (

    select

        *
    
    from {{ ref('fact_orders') }}

),

dates_dim as (

    select

        *
    
    from {{ ref('dim_calendar_date') }}
    
),

products_dim as (

    select

        *
    
    from {{ ref('dim_product') }}

),

sold_products_IDs_years_quantity as (

    select
        
        source.product_id,
        sum(source.quantity) as quantity_sum,
        dates_dim.date_year,

        rank() over (partition by dates_dim.date_year order by sum(source.quantity) desc) as quantity_rank_per_year
        
    from source
    left join dates_dim on
        dates_dim.date_id = source.order_date_id

    group by dates_dim.date_year, source.product_id
    -- order by dates_dim.date_year, sum(source.quantity) desc

),

top_sold_product_per_years as (

    select
    
        products_dim.product_id,
        products_dim.product_name,
        products_dim.product_category,
        products_dim.product_sub_category,

        sold_products_IDs_years_quantity.date_year,
        sold_products_IDs_years_quantity.quantity_sum
        
    from sold_products_IDs_years_quantity
    left join products_dim on
        products_dim.product_id = sold_products_IDs_years_quantity.product_id

    where quantity_rank_per_year = 1
    order by sold_products_IDs_years_quantity.date_year

)

select * from top_sold_product_per_years
