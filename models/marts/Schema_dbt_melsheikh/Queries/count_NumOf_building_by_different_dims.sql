-- Count the number of buildings by different dimensions (borough - lot - block).

with 

Location_Dim as (
    select

        min(location_id) as location_id , -- because it can't be aggregated unique IDs to I take the min/first id
        borough_name, tax_lot, tax_block

    from {{ ref('Dim_Location') }}
    group by borough_name, tax_lot, tax_block
),


borough_num as (
    select

        count(borough_name) num_building_borough,
        concat('Borough: ', borough_name) as  borough_name_idn

    from Location_Dim

    group by borough_name
),


Lot_num as (
    select

        count(tax_lot) num_building_tax_lot,
        concat('tax_lot: ', tax_lot) as  tax_lot_idn
        

    from Location_Dim

    group by tax_lot
),

Block_num as (
    select

        count(tax_block) num_building_tax_block,
        concat('tax_block: ', tax_block) as tax_block_idn
        

    from Location_Dim

    group by tax_block
),


show_data as (
    -- get the building data by the borough dimension
    select
        *
    from borough_num

    union

    -- get the building data by the Lot dimension
    select
        *
    from Lot_num

    union

    -- get the building data by the Block dimension
    select
        *
    from Block_num
)

select * from show_data