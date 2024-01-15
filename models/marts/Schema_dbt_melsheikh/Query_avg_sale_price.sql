with

Fct as (
    select

        Borough_number, Sale_price

    from {{ ref('Fct_Requerments') }}
),

Dim_Borough as (

    select * from {{ ref('Dim_Borough') }}

),

Avg_per_borough as (

    select

        Fct.borough_number,
        avg(Sale_price) as avg_sale_price

    from Fct
    group by borough_number

),

main_query as (

    select

        db.borough_name,
        Avg_pb.avg_sale_price

    from Avg_per_borough Avg_pb
    left join Dim_Borough db on Avg_pb.borough_number = db.borough_num_id

)

select * from main_query