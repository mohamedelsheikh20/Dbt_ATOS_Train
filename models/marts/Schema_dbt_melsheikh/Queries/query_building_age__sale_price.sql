-- determine the building age category based on year built, 
    -- and then use it to analyze the relationship between building age and sale price.


with 

Fact as (

    select 

        sale_price,
        YEAR_BUILT_DATE_ID
    
    from {{ ref('Fct_Requerments') }}
    where YEAR_BUILT_DATE_ID <> 1111 -- 1111 wrong, but 0 is unknown
),

age_cat_price as (
    select 
    
        YEAR_BUILT_DATE_ID as age_category,
        sum(sale_price) as sum_sale_price,
        avg(sale_price) as avg_sale_price,
        min(sale_price) as min_sale_price,
        max(sale_price) as max_sale_price

    from Fact
    group by YEAR_BUILT_DATE_ID
)

select * from age_cat_price