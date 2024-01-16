# New York City Property Sales 2016-2018
The New York City Department of Finance's Annualized Sales files lists properties that sold in the last twelve-month period in New York City for all tax classes.
data set from AWS S3 bucket [Data link](https://aws.amazon.com/marketplace/pp/prodview-27ompcouk2o6i?sr=0-3&ref_=beagle&applicationId=AWSMPContessa#overview).

* Key Fields: 
Neighborhood - Address - Sale price - Number of units - Square footage - Transaction date - Year built.
* Data Dictionary: [Data Dictionary link](https://www1.nyc.gov/assets/finance/downloads/pdf/07pdf/glossary_rsf071607.pdf).


---
## Introduction

Designing a Simple Star Schema using dbt for AWS S3 Dataset on Snowflake, 
Then transform the dataset into an analytical model by defining the fact and dimension tables and perform necessary transformations in the staging models to enable analytical queries.


---
## Getting Started

### Prerequisites
Before you begin, ensure that you have the following prerequisites:
* the mentioned data set on your snowflake account.
* connect your dbt account to those data using connector.

### Installation
You need to set up your `yml` files depending on your snowflake dataset, but in this case the data source are the same.

---
## Project Structure


### High-level structure of dbt project, including folders and files.
#### Dbt_ATOS_Train/
* models/
  * documentation/
    * documentation_file.md

  * models/
    * marts/
      * Schema_dbt_melsheikh/
        * Queries/
          * query_analyze_dist_diff_years_sale_price.sql
          * query_avg_sale_price_borough.sql
          * query_build_class_cat.sql
          * query_building_age__sale_price.sql
          * query_different_dim_building.sql
          * query_find_buildings_sold_multiple_times.sql
          * query_five_expensive_buildings.sql
          * query_neighbourhood_max_units.sql
          * query_tax_class_avg_sale.sql
          * query_total_sale_over_time.sql
          * query_window_fun_sale_price.sql
        * Fct_Requerments.sql
        
    * staging/
      * Schema_dbt_melsheikh/
        * Dimensions/
          * Dim_Building_class.sql
          * Dim_CalendarDate.sql
          * Dim_Location.sql
          * Dim_Tax_class.sql
        * __sources.yml
        * stg_dbt_melsheikh.yml
        * stg_dbt_melsheikh__nyc_property_sales.sql
  
  * dbt_project.yml



### Star Schema Structure used.

![Alt text](https://drive.google.com/file/d/1F07zKi02M4b8YX_Mym2ueTukQKFuxfIF/view?usp=sharing)

As the image shows there are `Fact table` with four `Dimension tables`:
1. Req Fact Table:
   1. Contain all foreign keys `FKs` to connect all dimensions.
   2. Contain all measures to be calculated later.
   3. Contain degenerated dimensions which have no table to add in.

  
2. Tax class dimension:
   1. Every property in the city is assigned to one of four tax classes `(From 1 to 4)`, based on the use of the property.
   2. There are 2 taxes class one on sale the property and one on the present date.


3. Building class dimension:
   1. The Building Classification is used to describe a property’s constructive use.
   2. There are 2 Building class one on sale the property and one on the present date.
   3. I added `building class category`  field include data so that users of the Rolling Sales Files can easily 
     identify similar properties by broad usage


4. Calendar date dimension:
   1. Dimension that holds all date in the date set (built year - sale date).
   

5. Location dimension:
   1. Info about where is exactly the property using:
      1. Borough.
      2. Tax (Block and Lot).
      3. Zip code.
      4. Neighbourhood.



---
## Configuration

All config will be found in `yml` files.

### Sources
#### path: models/staging/Schema_dbt_melsheikh/_sources.yml
#### Description:
* Name the main used schema `dbt_melsheikh`, database `pc_dbt_db` and table `nyc_property_sales` used in snowflake.
* Add description to the wanted parts.


### Staging yml
#### path: models/staging/Schema_dbt_melsheikh/stg_dbt_melsheikh.yml
#### Description:
* Define the model name `stg_dbt_melsheikh__nyc_property_sales` and let the space to some tests in the future.
* Add description to the wanted parts.

### Dbt project yml
#### path: dbt_project.yml
#### Description:
* Change the materialized of the staging and mart into table.
---
## Models
### Stage model:
(stage to clean, prepare and engineer the data).
#### path: models/staging/Schema_dbt_melsheikh/stg_dbt_melsheikh__nyc_property_sales.sql
#### Description:
* Select the main table data from `_sources.yml` in the same path.
* first of all there are some cleaned data columns: 
  * Borough_number: No nulls - only 5 distinct values.
  * Borough_name: No nulls - only 5 distinct values.
  * Sale_date: No nulls - only valid 2016 to 2018 dates.
  * Lot: No nulls - all is numeric values.
  * Block: No nulls - all is numeric values.
  * Building_class_category: No nulls - 91 distinct values.
  * Neighborhood: No nulls - 258 distinct values.
  * Tax_class_at_sale: No nulls - only numeric (1 to 4).
  * Building_class_at_sale: No nulls - data cleaned.


* second there are columns needed to be cleaned:
  (any `-` can't be cast, so it converted into null)

  * zip code, year built: Replace null values with zero.

  * Sale price:
    * Using `trim` to remove and before/after spaces.
    * Using `replace` to replace `,` and `$` with empty.
    * Using `try_cast` to cast into integer and what can't be cast make it null.

  * Square feets (Gross, Land) same operation:
    * Using `trim` to remove and before/after spaces.
    * Using `replace` to replace `,` with empty.
    * Using `try_cast` to cast into integer and what can't be cast make it null.

  * Building class at present:  If it null and building at sale contain value so duplicate the value.
    * Tax class at present: If it null and tax at sale contain value so duplicate the value. 

  * units (Residential_units, Commercial_units, Total_units): same
    operation for the 3:
    * Using `trim` to remove and before/after spaces.
    * Using `replace` to replace `,` with empty.
    * Using `try_cast` to cast into integer and what can't be cast make it null.
    * Using `case` to fill the unfilled unit (only if the other two are contain data).

  * Address: Remove the apartment number from the address (what after `,`).
    * Apartment_number: Have 3 values, main apartment value or get it from address or `UnKnown`.

    

### Dimension models: 
(all dimensions take the same source from the only exist stage model)

#### Tax class dimension
#### path: models/staging/Schema_dbt_melsheikh/Dimensions/Dim_Tax_class.sql
#### Description:
* Using related distinct columns:
  * tax_class_at_sale,
  * tax_class_at_present
* Add primary key generated key using the row number.



#### Building class dimension
#### path: models/staging/Schema_dbt_melsheikh/Dimensions/Dim_Building_class.sql
#### Description:
* Using related distinct columns:
  * building_class_at_present.
  * building_class_at_sale.
  * Building_class_category.
* Add primary key generated key using the row number.


#### Calendar date dimension
#### path: models/staging/Schema_dbt_melsheikh/Dimensions/Dim_CalendarDate.sql
#### Description:
* Using related distinct columns of any date:
  * building_class_at_presentSale_date.
  * property_year_built.
* Add primary key:
  * Sale_date: same date but concatenated like `YYYYMMDD`.
  * property_year_built: year date like `YYYY` with setting day and month with zeros.
* Union both dates id to be stored in the dimension.


#### Location dimension
#### path: models/staging/Schema_dbt_melsheikh/Dimensions/Dim_Location.sql
#### Description:
* Using related distinct columns:
  * borough_name.
  * neighborhood.
  * property_zip_code.
  * tax_lot.
  * tax_block.
* Add primary key generated key using the row number.

### Fact model: 
Fact table model which use the previous dimension tables to join the primary
key of them as a foreign key in fact table, also use the stage of cleaning date
which is in the source. 

#### Fct Requirements fact
#### path: models/marts/Schema_dbt_melsheikh/Fct_Requerments.sql
#### Description:
1. Main fact measures columns:
   * Sale_price, Gross_square_feet_int, Land_square_feet_int, Residential_units_int, Commercial_units_int, Total_units_int, Address, apartment_number.


2. Calender foreign keys:
   1. Can be generated without the need of joining.
   2. Using the same structured used in the `Calendar Dim`.


3. Foreign keys from the dimensions:
   1. Using left join to get all the fact rows.
   2. Join is on all distinct columns considered in the mentioned dimension.
---
## Queries
### Query 1: 

---
## Tests

There is no tests implemented yet.

---
## Common Issues
### Main source snowflake Table:
1. Column `Easement`: Column contain nulls only. 
   * I removed it from the stage for now.
  

2. Column `Borough_number`: Column have repeated un-used data.
each Borough have corresponding number, also this number cannot be added as foreign key 
   * I removed it from the stage for now.


3. Column `Year built`: Column have wrong row data.
   * there is in date id = `20180222` diff year in negative (mean that the house sold before it built).


---
## License
* MIT License.
* This is just a practice.
