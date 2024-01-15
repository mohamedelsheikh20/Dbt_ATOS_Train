with

source as (
    select * from {{ ref('stg_dbt_melsheikh__nyc_property_sales') }}
),


sale_date as (
    select 

        TO_NUMBER(TO_VARCHAR(Sale_date, 'YYYYMMDD')) as date_yyyymmdd_id,
        day(Sale_date) as date_day,
        month(Sale_date) as date_month,
        year(Sale_date) as date_year
        
    from source
),

built_date as (
    select 
    
        cast(property_year_built as int) as date_yyyymmdd_id,
        00 as date_day,
        00 as date_month,
        cast(property_year_built as int) as date_year

    from source
),

Calendar_dim as (
    -- select * from source
    select * from sale_date
    union
    select * from built_date
)

select * from Calendar_dim