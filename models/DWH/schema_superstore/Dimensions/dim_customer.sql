with

source as (

    select
        
        *
        
    from {{ ref('stg_schema_superstore__superstore_sample') }}
),

customer_set as (

    select distinct
        
        customer_id,
        customer_name,
        segment as customer_segment

        -- I have splitted the location info because one customer may have more that 10 locations
        
    from source

),

dim_customer as (

    select
        
        *
    
    from customer_set
)

select * from dim_customer