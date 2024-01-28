-- Query to find the total sales for each category.
-- this query can be for product (category, sub_cat), customer (segment, name) or the location.

with

source as (

    select

        *
    
    from {{ ref('fact_orders') }}

),

products_dim as (

    select

        *
    
    from {{ ref('dim_product') }}

),

total_sales_per_category as (

    select

        products_dim.product_category,

        sum(source.sales) as sales_per_category
        
    from source
    left join products_dim on
        products_dim.product_id = source.product_id

    group by products_dim.product_category

)

select * from total_sales_per_category

