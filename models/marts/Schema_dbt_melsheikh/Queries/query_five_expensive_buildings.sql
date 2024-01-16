with 

Fact as (

    select 

     sale_price,
     location_id
    
    from {{ ref('Fct_Requerments') }}
),

Location_Dim as (
    select

        *

    from {{ ref('Dim_Location') }}
),



join_Dim_Fact as (

    select

    sale_price,
    F.location_id,
    borough_name,
    neighborhood,
    property_zip_code,
    tax_lot,
    tax_block

    from Fact F
    left join Location_Dim LD on LD.location_id = F.location_id
),

-- select * from join_Dim_Fact


neighbourhood_num as (
    select
        
        -- *
        sum(sale_price) as sum_sale_salary,
        -- location_id
        borough_name,
        neighborhood,
        property_zip_code,
        tax_lot,
        tax_block

    from join_Dim_Fact
    -- group by location_id  -- why can't I group by location ID
    group by borough_name, neighborhood,
        property_zip_code, tax_lot, tax_block

    having sum_sale_salary is not null
),


show_data as (
    select
    
        *
    
    from neighbourhood_num
    order by sum_sale_salary desc
    limit 5
)

select * from show_data