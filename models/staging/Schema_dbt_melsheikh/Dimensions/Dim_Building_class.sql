with

source as (
    select * from {{ ref('stg_dbt_melsheikh__nyc_property_sales') }}
),

building_class_at_sale_distinct as (
    select distinct 

        building_class_at_sale
    
    from source
),

building_class_at_present_distinct as (
    select distinct 

        building_class_at_present
    
    from source
),

Building_combination_dim as (
    select

        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as Building_class_ID,
        sale.building_class_at_sale,
        present.building_class_at_present
        
    from building_class_at_sale_distinct sale
    cross join building_class_at_present_distinct present
)


select * from Building_combination_dim