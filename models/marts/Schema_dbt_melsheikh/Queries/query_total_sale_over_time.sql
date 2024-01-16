with 

Fact as (

    select 

     sale_price,
     sale_date_id
    
    from fct_requerments
),

Calendar_Dim as (
    select

        *

    from dim_calendardate
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


sales_2016 as (
    select

        2016 as year,
        date_month as months,
        sum(sale_price) as sale_2016_per_month

    from join_Dim_Fact
    where date_year = 2016
    group by date_month
),


sales_2017 as (
    select

        2017 as year,
        date_month as months,
        sum(sale_price) as sale_2017_per_month

    from join_Dim_Fact
    where date_year = 2017
    group by date_month
),


sales_2018 as (
    select

        2018 as year,
        date_month as months,
        sum(sale_price) as sale_2018_per_month

    from join_Dim_Fact
    where date_year = 2018
    group by date_month
),

years_months_sales as (
    select
        *
    from sales_2016

    union

    select
        *
    from sales_2017

    union

    select
        *
    from sales_2018
)

select * from years_months_sales