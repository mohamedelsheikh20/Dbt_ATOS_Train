-- Create a new column with the difference in years between sale date and year built. 
--      Group the data by this new column and analyze the distribution of sale price.


with 

Fact as (

    select 

        sale_price,
        sale_date_id,
        year_built_date
    
    from {{ ref('Fct_Requerments') }}

    -- in year_built_date there are unwanted values like (1111, 0) so we neglect them
    where year_built_date > 1111
),


get_year_diff as (

    select 

        sale_price,
        -- sale_date_id like "20181212" / 10000 = 2018.1212 using cast int will be the year "2018"
        -- year_built_date_id will be like "1996"
        (cast(sale_date_id / 10000 as int) - year_built_date) as diff_year

    
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