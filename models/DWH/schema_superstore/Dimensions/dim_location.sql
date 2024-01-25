with

source as (

    select
        
        *
        
    from {{ ref('stg_schema_superstore__superstore_sample') }}
),

location_set as (

    select distinct

        country as location_country,
        city as location_city,
        state as location_state,
        postal_code as location_postal_code,
        region as location_region
        
    from source

),

dim_location as (

    select
        
        row_number() over (order by (select null)) as location_id,
        *
    
    from location_set
)

select * from dim_location