with 

Fact as (

    select 

     sale_price,
     sale_date_id,
     year_built_date_id
    
    from {{ ref('Fct_Requerments') }}
),


get_year_diff as (

    select 

        sale_price,
        (cast(sale_date_id / 10000 as int) - year_built_date_id) as diff_year

    
    from Fact
    where diff_year > 0 -- there is in date id = 20180222 diff year in negative

),

analyze_distribution as (
    select
    
        diff_year,
        sum(sale_price) as sum_sale_price,
        avg(sale_price) as avg_sale_price,
        min(sale_price) as min_sale_price,
        max(sale_price) as max_sale_price
        
    from get_year_diff
    group by diff_year

)

select * from analyze_distribution