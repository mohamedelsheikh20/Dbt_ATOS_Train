-- Identify the building class category with the highest average land square feet.

with 

Fact as (

    select 

     building_class_id,
     land_square_feet_int
    
    from {{ ref('Fct_Requerments') }}
),


Building_Dim as (

    select
    
        building_class_id,
        building_class_category

    from {{ ref('Dim_Building_class') }}
),


avg_land_square as (
    select

        avg(land_square_feet_int) as avg_land_square_feet,
        BD.building_class_category

    from Fact f
    left join Building_Dim BD on BD.building_class_id = f.building_class_id

    where f.land_square_feet_int is not null  -- to order them desc
    group by BD.building_class_category
),

max_avg_land_square as (
    select
        avg_land_square_feet,
        building_class_category

    from avg_land_square
    order by avg_land_square_feet desc
    limit 1
)

select * from max_avg_land_square