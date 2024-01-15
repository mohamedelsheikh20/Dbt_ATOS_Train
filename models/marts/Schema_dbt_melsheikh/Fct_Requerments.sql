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

join_forign_keys as (
    select

        -- Borough_number are already exist in the source + the other columns
        borough_number,
        Sale_price,
        Gross_square_feet_int,
        Land_square_feet_int,
        Residential_units_int,
        Commercial_units_int,
        Total_units_int,
        Address,
        apartment_number,

        -- Building class
        tx.Tax_class_ID,

        -- Tax class
        bld.Building_class_ID,


        -- Calendar date
        TO_NUMBER(TO_VARCHAR(Sale_date, 'YYYYMMDD')) as Sale_Date_ID,
        cast(property_year_built as int) as Year_built_Date_ID

    from source s
    
    -- add the tax class foriegn key
    left join tax_class_data tx on
        tx.tax_class_at_sale = s.tax_class_at_sale and
        tx.tax_class_at_present = s.tax_class_at_present

    -- add the building class foriegn key
    left join building_class_data bld on
        bld.building_class_at_sale = s.building_class_at_sale and
        bld.building_class_at_present = s.building_class_at_present

)

select * from join_forign_keys
