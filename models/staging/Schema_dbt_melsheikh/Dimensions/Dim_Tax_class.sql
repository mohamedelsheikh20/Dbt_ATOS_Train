with

source as (
    select * from {{ ref('stg_dbt_melsheikh__nyc_property_sales') }}
),

Tax_class_distinct as (
    select distinct

        tax_class_at_sale,
        tax_class_at_present
    
    from source
),

Tax_combination_dim as (
    select 

        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as Tax_class_ID,
        tax_class_at_sale,
        tax_class_at_present
        
    from Tax_class_distinct
)

select * from Tax_combination_dim