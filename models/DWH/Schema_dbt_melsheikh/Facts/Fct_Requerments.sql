with

source as (
    select * from {{ ref('stg_dbt_melsheikh__nyc_property_sales') }}
),

tax_class_data as (
    select * from {{ ref('Dim_Tax_class') }}
),

building_class_data as (
    select * from {{ ref('Dim_Building_class') }}
),

location_data as (
    select * from {{ ref('Dim_Location') }}
),

join_forign_keys as (
    select

        Sale_price,
        Gross_square_feet_int,
        Land_square_feet_int,
        Residential_units_int,
        Commercial_units_int,
        Total_units_int,
        
        -- degenerate dim
        Address,
        apartment_number,
        cast(property_year_built as int) as year_built_date,

        -- Building class dim
        tx.Tax_class_ID,

        -- Tax class dim
        bld.Building_class_ID,

        -- Location dim
        L.Location_ID,


        -- Calendar date dim
        to_number(to_varchar(Sale_date, 'YYYYMMDD')) as Sale_Date_ID

    from source s
    
    -- add the tax class foriegn key
    left join tax_class_data tx on
        tx.tax_class_at_sale = s.tax_class_at_sale and
        tx.tax_class_at_present = s.tax_class_at_present

    -- add the building class foriegn key
    left join building_class_data bld on
        bld.building_class_at_sale = s.building_class_at_sale and
        bld.building_class_at_present = s.building_class_at_present and
        bld.building_class_category = s.building_class_category

    -- add the location foriegn key
    left join location_data L on
        L.borough_name = s.borough_name and
        L.neighborhood = s.neighborhood and
        L.property_zip_code = s.property_zip_code and
        L.tax_lot = s.tax_lot and
        L.tax_block = s.tax_block

)

select * from join_forign_keys