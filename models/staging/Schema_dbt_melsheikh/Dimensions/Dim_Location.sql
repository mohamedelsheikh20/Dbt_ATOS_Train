with

source as (
    select * from {{ ref('stg_dbt_melsheikh__nyc_property_sales') }}
),


Location_data_distinct as (
    select distinct

        borough_name,
        NEIGHBORHOOD,
        PROPERTY_ZIP_CODE,
        TAX_LOT,
        TAX_BLOCK
        
    from source
),

Location_dim as (
    select 

        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as Location_ID,
        *
        
    from Location_data_distinct
)

select * from Location_dim