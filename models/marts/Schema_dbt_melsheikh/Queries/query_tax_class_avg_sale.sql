-- Group the data by tax class at present and tax class at time of sale and compare the average sale price for each combination.

with 

Fact as (

    select 

        sale_price,
        TAX_CLASS_ID
    
    from {{ ref('Fct_Requerments') }}
),

Tax_Dim as (
    select

        *

    from {{ ref('Dim_Tax_class') }}
),


join_Dim_Fact as (

    select

        sale_price,
        tax_class_at_sale,
        tax_class_at_present

    from Fact F
    left join Tax_Dim TD on TD.tax_class_id = F.tax_class_id
),


tax_comb_avg_sale as (
    select

        tax_class_at_sale, 
        tax_class_at_present,
        avg(sale_price) as avg_sale_price
        

    from join_Dim_Fact
    group by tax_class_at_sale, tax_class_at_present
)

select * from tax_comb_avg_sale