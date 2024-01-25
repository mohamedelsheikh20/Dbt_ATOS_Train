with

source as (

    select
        
        *
        
    from {{ ref('stg_schema_superstore__superstore_sample') }}
),


-- I found repeated IDs with different name (32 rows of 1862), 
        -- as I don't know the business use case so I will keep one name of them

product_set as (

    select

        product_id,
        max(product_name) as product_name,  -- suppose that use case need the max
        max(category) as product_category,
        max(sub_category) as product_sub_category

    from source
    group by product_id

),


dim_product as (

    select
        
        product_id,
        product_name,
        product_category,
        product_sub_category
    
    from product_set
)

select * from dim_product
