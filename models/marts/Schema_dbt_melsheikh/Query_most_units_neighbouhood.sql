with

Fct as (
    select

        neighborhood, total_units_int

    from {{ ref('Fct_Requerments') }}
),


Sum_per_neighborhood as (

    select

        neighborhood,
        sum(total_units_int) as sum_total_units

    from Fct
    group by neighborhood
    having sum_total_units is not null  -- total units contain nulls
),

main_query as (

    select 

        neighborhood,
        sum_total_units
        -- max(sum_total_units) as max_neighborhood_units  -- there is a problem (check)

    from Sum_per_neighborhood
    order by sum_total_units desc
    limit 1
)

select * from main_query