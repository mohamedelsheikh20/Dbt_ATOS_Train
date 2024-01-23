-- Implement a logic to find buildings sold multiple times and compare their sale price across each transaction.


with 

Fact as (

    select 

        sale_price,
        location_id
    
    from {{ ref('Fct_Requerments') }}
),

Location_Dim as (
    select

        min(location_id) AS original_location_id, -- get the first (min) Id to use
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
    -- because of group by borough_name, tax_lot, tax_block and neglect columns neighbourhood - zip code
    inner join Location_Dim LD on LD.original_location_id = F.location_id
),

sale_salary_per_building as (
    select  -- distinct
        
        sale_price,
        borough_name,
        tax_lot,
        tax_block,

        -- rank sale price for each bulding (borough, lot, block) from smaller
        rank() over (partition by borough_name, tax_lot, tax_block order by sale_price) as price_rank


    from join_Dim_Fact
),


show_data as (
    select
    
        *
    
    from sale_salary_per_building
)

select * from show_data