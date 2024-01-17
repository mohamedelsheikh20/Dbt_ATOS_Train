-- Identify the top 5 most expensive buildings based on sale price.


with 

Fact as (

    select 

        sale_price,
        location_id
    
    from {{ ref('Fct_Requerments') }}
),

Location_Dim as (
    select

        min(location_id) as original_location_id, -- get the first (min) Id to use
        borough_name, tax_lot, tax_block

    from {{ ref('Dim_Location') }}
    group by borough_name, tax_lot, tax_block
),
 

join_Dim_Fact as (

    select

        sale_price,
        LD.original_location_id,
        borough_name,
        tax_lot,
        tax_block

    from Fact F
    -- here I used inner join to neglect all nulls location id
    -- because of group by BOROUGH_NAME, TAX_LOT, TAX_BLOCK and neglect neigh - zip code
    inner join Location_Dim LD on LD.original_location_id = F.location_id
),

sale_salary_per_building as (
    select
        
        sum(sale_price) as sum_sale_salary,
        borough_name,
        tax_lot,
        tax_block

    from join_Dim_Fact
    
    group by borough_name, tax_lot, tax_block  -- because building define by borough, lot, block
    having sum_sale_salary is not null
),


show_data as (
    
    select
    
        *
    
    from sale_salary_per_building
    order by sum_sale_salary desc
    limit 5
)

select * from show_data