with

source as (
    select * from {{ ref('stg_dbt_melsheikh__nyc_property_sales') }}
),

building_class_distinct as (
    select distinct 

        building_class_at_present,
        building_class_at_sale,
        Building_class_category
    
    from source
),

Building_combination_dim as (
    select

        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as Building_class_ID,
        *
        
    from building_class_distinct
)


select * from Building_combination_dim