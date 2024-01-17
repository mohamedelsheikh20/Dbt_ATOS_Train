with

source as (
    select * from {{ ref('stg_dbt_melsheikh__nyc_property_sales') }}
),


sale_date as (
    select 

        to_number(to_varchar(Sale_date, 'YYYYMMDD')) as date_yyyymmdd_id,
        day(Sale_date) as date_day,
        month(Sale_date) as date_month,
        year(Sale_date) as date_year
        
    from source
),

Calendar_dim as (

    select

        *
    
    from sale_date

)

select * from Calendar_dim