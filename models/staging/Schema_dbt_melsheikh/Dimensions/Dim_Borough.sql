with

source as (
    select * from {{ ref('stg_dbt_melsheikh__nyc_property_sales') }}
),


Borough_Dim as (
    select distinct

        borough_number as Borough_num_ID,
        borough_name
        
    from source
)

select * from Borough_Dim