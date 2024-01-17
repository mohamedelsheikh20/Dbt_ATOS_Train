with

source as (

    select * from {{ source('dbt_melsheikh', 'nyc_property_sales') }}

),


-- CTE Transformations
cleaned_data as (
    select

        -- cleaned columns
        "Borough_number" as Borough_number,
        "Borough_name" as Borough_name,
        "Sale_date" as Sale_date,
        "Lot" as Tax_lot,
        "Block" as Tax_block,
        "Building_class_category" as Building_class_category,
        "Neighborhood" as Neighborhood,

        
        -- hanlde year built
        coalesce("Year_built", 0) as property_year_built, 

        
        -- handle Sale price
        try_cast(replace(replace(TRIM("Sale_price", ' '),',', ''),'$', '')as integer) as sale_price_int,

        
        -- handle Land_square_feet
        try_cast(replace(trim("Land_square_feet", ' '), ',', '') as integer) as Land_square_feet_int,

        
        -- handle Land square feet
        try_cast(replace(trim("Gross_square_feet", ' '), ',', '') as integer) as Gross_square_feet_int,

        
        -- handle Building class 
        "Building_class_at_sale" as Building_class_at_sale,
        -- replace null at present with at sale (no nulls in at sale data)
        coalesce("Building_class_at_present", "Building_class_at_sale") as Building_class_at_present,

        
        -- handle Tax class
        "Tax_class_at_sale" as Tax_class_at_sale,
        -- replace null at present with at sale (no nulls in at sale data)
        coalesce("Tax_class_at_present", TO_VARCHAR("Tax_class_at_sale")) as Tax_class_at_present,

        
        -- handle units
        -- handle Residential_units
        try_cast(replace(trim("Residential_units", ' '), ',', '') as integer) as Residential_units,
        -- handle Commercial_units
        try_cast(replace(trim("Commercial_units", ' '), ',', '') as integer) as Commercial_units,
        -- handle Total_units
        try_cast(replace(trim("Total_units", ' '), ',', '') as integer) as Total_units,


        -- Residential_units
        (case 
            when 
            Total_units is not null and 
            Commercial_units is not null and
            Residential_units is null
            then 0 
            else Residential_units end) as Residential_units_int,

        -- Commercial_units
        (case 
            when 
            Total_units is not null and 
            Residential_units is not null and
            Commercial_units is null
            then 0 
            else Commercial_units end) as Commercial_units_int,
            
        -- Total_units
        (case 
            when 
            Residential_units is not null and 
            Commercial_units is not null and
            Total_units is null
            then 0 
            else Total_units end) as Total_units_int,


        -- handle Address and Apartment_number
        -- main "Apartment_number" column
        "Apartment_number" as main_apartment_number,
        
        -- extract appertment number from the address, and replace all null with 'unknown'
        (case 
            when trim(SPLIT(trim("Address", ' '), ',')[1], ' ') = '' or
                trim(SPLIT(trim("Address", ' '), ',')[1], ' ') is null
            then 'UnKnown'
            
            else trim(split(trim("Address", ' '), ',')[1], ' ') end
            ) as Apartment_number_address,
        
        -- get street number and details from the address
        SPLIT(trim("Address", ' '), ',')[0] as Address,


        (case 
            when main_apartment_number is null 
            then Apartment_number_address 
            else main_apartment_number end) as Apartment_number,

        -- handle nulls of zip code (replace by 0)
        -- "Zip_code" as property_zip_code,
        coalesce("Zip_code", 0) as property_zip_code

    FROM source

),


transformed as (
    select 

        Borough_number,
        Borough_name,
        
        Tax_lot,
        Tax_block,
        
        Building_class_category,
        Neighborhood,
        
        property_year_built, 
        Sale_date,
        
        sale_price_int as Sale_price,
        
        Land_square_feet_int,
        Gross_square_feet_int,

        Building_class_at_sale,
        Building_class_at_present,

        Tax_class_at_sale,
        Tax_class_at_present,

        Residential_units_int,
        Commercial_units_int,
        Total_units_int,

        Address,
        Apartment_number,
        property_zip_code

        
    from cleaned_data
)

select * from transformed