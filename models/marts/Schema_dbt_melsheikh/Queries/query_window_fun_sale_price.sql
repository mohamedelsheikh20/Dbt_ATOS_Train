-- Use a window function to calculate the running total of Sales price.

with 

Fact as (

    select 

        sale_price,
        sale_date_id
    
    from {{ ref('Fct_Requerments') }}
),

Calendar_Dim as (
    select

        *

    from {{ ref('Dim_CalendarDate') }}

),


join_Dim_Fact as (

    select
    
        sale_price, 
        date_day,
        date_month,
        date_year

    from Fact F
    left join Calendar_Dim CD on CD.date_yyyymmdd_id = F.sale_date_id
),


running_total_sale_price as (
    SELECT
        date_year,
        date_month,
        date_day,
        sale_price,
        SUM(sale_price) OVER (ORDER BY date_year, date_month, date_day) AS running_total
    
    FROM join_Dim_Fact

    -- where sale_price is not null
    order by date_year, date_month, date_day
)

select * from running_total_sale_price