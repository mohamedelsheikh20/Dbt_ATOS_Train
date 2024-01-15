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
    select

        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as Tax_class_ID,
        sale.tax_class_at_sale,
        present.tax_class_at_present
        
    from Tax_class_at_sale_distinct sale
    cross join Tax_class_at_present_distinct present
)

select * from Tax_combination_dim