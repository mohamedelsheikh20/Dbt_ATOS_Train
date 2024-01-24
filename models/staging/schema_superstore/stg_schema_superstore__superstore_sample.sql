with

source as (

    select

        * 
    
    from {{ source('schema_superstore', 'superstore_sample') }}

)


-- there is no cleaning phases needed to this dataset.
select * from source