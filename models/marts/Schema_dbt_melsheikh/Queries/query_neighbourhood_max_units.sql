-- Find the neighborhood with the most total units.

with 

Fact as (

    select 

     total_units_int,
     location_id
    
    from {{ ref('Fct_Requerments') }}
),

Location_Dim as (

    select

        neighborhood,
        location_id

    from {{ ref('Dim_Location') }}
),


Max_units_neighbourhoods as (
    select
        max(f.total_units_int) as neighbouhood_max,
        LD.neighborhood

    from Fact f
    left join Location_Dim LD on LD.location_id = f.location_id

    where f.total_units_int is not null and f.total_units_int <> 0
    group by LD.neighborhood
),

Max_neighbourhood as (
    select
        neighbouhood_max,
        neighborhood

    from Max_units_neighbourhoods
    order by neighbouhood_max desc
    limit 1
)

select * from Max_neighbourhood