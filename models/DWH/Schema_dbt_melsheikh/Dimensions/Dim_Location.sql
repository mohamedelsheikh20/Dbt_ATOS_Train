with

source as (
    select * from {{ ref('stg_dbt_melsheikh__nyc_property_sales') }}
),


Location_data_distinct as (
    select distinct

        borough_name,
        neighborhood,
        property_zip_code,
        tax_lot,
        tax_block
        
    from source
),

Location_dim as (
    select 

        row_number() over (order by (select null)) as Location_ID,
        *
        
    from Location_data_distinct
)

select * from Location_dim