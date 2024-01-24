with

source as (

    select

        *
    
    from {{ ref('stg_schema_superstore__superstore_sample') }}

),

order_date_set as (

    select distinct  -- get the unique dates only
        
        to_number(to_varchar(order_date, 'YYYYMMDD')) as date_id,
        order_date as main_date,
        day(order_date) as date_day,
        month(order_date) as date_month,
        year(order_date) as date_year
        
    from source

),

ship_date_set as (

    select distinct  -- get the unique dates only
        
        to_number(to_varchar(ship_date, 'YYYYMMDD')) as date_id,
        ship_date as main_date,
        day(ship_date) as date_day,
        month(ship_date) as date_month,
        year(ship_date) as date_year
        
    from source

),

dim_calendar_date as (

    select
        
        *
    
    from order_date_set

    union    -- using union to remove the duplication (also use tests in yml to be sure)

    select
        
        *
    
    from ship_date_set
)

select * from dim_calendar_date