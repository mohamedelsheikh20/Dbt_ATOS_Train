with

source as (
    select * from {{ ref('stg_dbt_melsheikh__nyc_property_sales') }}
),

Tax_class_at_sale_distinct as (
    select distinct 
        Tax_class_at_sale
    
    from source
),

Tax_class_at_present_distinct as (
    select distinct 
        Tax_class_at_present
    
    from source
),

Tax_combination_dim as (

    select  ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as generated_key, *
    from Tax_class_at_sale_distinct
    cross join Tax_class_at_present_distinct
)

select * from Tax_combination_dim