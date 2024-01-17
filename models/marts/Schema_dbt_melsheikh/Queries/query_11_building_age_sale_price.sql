-- determine the building age category based on year built, 
    -- and then use it to analyze the relationship between building age and sale price.


with 

Fact as (

    select 

        sale_price,
        year_built_date_id
    
    from {{ ref('Fct_Requerments') }}
    where year_built_date_id <> 1111 -- 1111 wrong, but 0 is unknown
),

age_cat_price as (
    select 
    
        year_built_date_id as age_category,
        sum(sale_price) as sum_sale_price,
        avg(sale_price) as avg_sale_price,
        min(sale_price) as min_sale_price,
        max(sale_price) as max_sale_price

    from Fact
    group by year_built_date_id
)

select * from age_cat_price