-- Calculate the average sale price per borough.


with 

Fact as (

    select 

        sale_price,
        location_id
    
    from {{ ref('Fct_Requerments') }}

),

Location_Dim as (

    select

        borough_name,
        location_id

    from {{ ref('Dim_Location') }}
),

Avg_sale_price_borough as (
    select

        LD.borough_name,
        avg(f.sale_price) as avg_sale_price

    from Fact f
    left join Location_Dim LD on
        LD.location_id = f.location_id

    group by LD.borough_name
)

select * from Avg_sale_price_borough