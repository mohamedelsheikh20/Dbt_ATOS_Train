-- Query to get customers who made more than one order in the same day
-- or can get the top csutomers who made orders

with

source as (

    select

        *
    
    from {{ ref('fact_orders') }}

),

customers_dim as (

    select

        *
    
    from {{ ref('dim_customer') }}

),

calendar_date_dim as (

    select

        *
    
    from {{ ref('dim_calendar_date') }}

),

distinct_order_per_customer as (

    select

        customer_id,
        count(distinct order_id) as distinct_order_ids,
        order_date_id
        
    from source
    group by customer_id, order_date_id
    having distinct_order_ids > 1
    
),

test as (

    select

        customers_dim.customer_name,
        distinct_order_per_customer.distinct_order_ids,
        calendar_date_dim.main_date,
        calendar_date_dim.date_day,
        calendar_date_dim.date_month,
        calendar_date_dim.date_year
        
        
    from distinct_order_per_customer

    left join customers_dim on
        customers_dim.customer_id = distinct_order_per_customer.customer_id

    left join calendar_date_dim on 
        calendar_date_dim.date_id = distinct_order_per_customer.order_date_id

)

select * from test

