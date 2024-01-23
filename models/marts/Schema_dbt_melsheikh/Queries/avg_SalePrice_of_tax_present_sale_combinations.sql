-- Group the data by tax class at present and tax class at time of sale and compare the average sale price for each combination.

with 

Fact as (
    select 

        sale_price,
        tax_class_id
    
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


tax_avg_sale_at_sale as (
    select

        tax_class_at_sale, 
        avg(sale_price) as avg_sale_price_tax_at_sale
        

    from join_Dim_Fact
    group by tax_class_at_sale
    order by tax_class_at_sale
),


tax_avg_sale_at_present as (
    select

        -- get the sub class to 1,  1A, 1B, .... IN GROUP 1 etc..
        left(tax_class_at_present, 1) as sub_tax_class_at_present,
        avg(sale_price) as avg_sale_price_tax_at_present
        

    from join_Dim_Fact
    group by sub_tax_class_at_present
    order by sub_tax_class_at_present
),


compare_results as (

    select

        tax_avg_sale_at_sale.tax_class_at_sale,
        tax_avg_sale_at_sale.avg_sale_price_tax_at_sale,
        
        tax_avg_sale_at_present.sub_tax_class_at_present,
        tax_avg_sale_at_present.avg_sale_price_tax_at_present,

        -- check the difference between the avg
        tax_avg_sale_at_present.avg_sale_price_tax_at_present - tax_avg_sale_at_sale.avg_sale_price_tax_at_sale AS diff_compare_column,


        -- check the bigger avg
        case
            when tax_avg_sale_at_sale.avg_sale_price_tax_at_sale > tax_avg_sale_at_present.avg_sale_price_tax_at_present 
            then 'At Sale is Bigger'
            
            when tax_avg_sale_at_sale.avg_sale_price_tax_at_sale < tax_avg_sale_at_present.avg_sale_price_tax_at_present 
            then 'At Present is Bigger'
            
            else 'Equal'
        end as bigger_compare_result
        
    
    from tax_avg_sale_at_sale
    
    cross join tax_avg_sale_at_present 
)


select * from compare_results
