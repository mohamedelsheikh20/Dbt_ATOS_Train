with 

Location_Dim as (

    select
    
        *

    from {{ ref('Dim_Location') }}
),


borough_num as (
    select

        count(borough_name) num_building_borough,
        concat('Borough: ', borough_name) as  borough_name_idn

    from Location_Dim

    group by borough_name
),


neighbourhood_num as (
    select

        count(Neighborhood) num_building_neighbourhood,
        concat('Neighborhood: ', Neighborhood) as  Neighborhood_idn
        

    from Location_Dim

    group by Neighborhood
),


show_data as (
    select
        *
    from borough_num

    union

    select
        *
    from neighbourhood_num
)

select * from show_data