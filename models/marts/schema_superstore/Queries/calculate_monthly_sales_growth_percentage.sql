-- Query to calculate the monthly sales growth percentage
-- monthly sales growth percentage equation = ((this month - previous month) / previous month) * 100

with

source as (

    select

        *
    
    from {{ ref('fact_orders') }}

),

calendar_date_dim as (

    select

        *
    
    from {{ ref('dim_calendar_date') }}

),

-- get the sales per each month in each year.
sales_per_month as (

    select
    
        calendar_date_dim.date_year,
        calendar_date_dim.date_month,
        sum(source.sales) as month_sales
        
    from source
    left join calendar_date_dim on
        calendar_date_dim.date_id = source.order_date_id

    group by date_year, date_month
    
),

-- get the sales per each previous month using window function and (lag).
sales_per_month_with_previous_month as (

    select

        date_year,
        date_month,
        month_sales,
        lag(month_sales) over (order by date_year, date_month) as previous_month_Sales
    
    from sales_per_month
    
),

-- do the equation in generated columns.
monthly_groth_percentage_equation as (

    select
    
        date_year,
        date_month,
        month_sales,
        previous_month_Sales,
        ((month_sales - previous_month_Sales) / previous_month_Sales) * 100 as monthly_growth_percentage
    
    from sales_per_month_with_previous_month

),

-- round the output and add '%' to it, so it will be readable.
transformed as (

    select

        date_year,
        date_month,
        month_sales,
        previous_month_Sales,
        concat(round(monthly_growth_percentage, 2), '%') as RoundedPercentage
    
    from monthly_groth_percentage_equation

)

select * from transformed
