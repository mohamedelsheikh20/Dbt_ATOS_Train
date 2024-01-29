-- Query to find the average profit margin for each product sub-category.
-- using the profit margin equation (profit / sales)

with

source as (

    select

        *
    
    from {{ ref('fact_orders') }}

),

product_dim as (

    select

        *
    
    from {{ ref('dim_product') }}

),


avg_profit_margin_per_subcategory as (

    select
    
        product_dim.product_sub_category,
        avg(source.profit / source.sales) as avg_profit_margin
    
    from source
    left join product_dim on
        product_dim.product_id = source.product_id
    
    group by product_dim.product_sub_category
    
)

select * from avg_profit_margin_per_subcategory
