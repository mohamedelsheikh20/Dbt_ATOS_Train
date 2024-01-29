# Superstore Dataset

* The Superstore dataset provides an opportunity to conduct an in-depth analysis of sales performance, enabling businesses to gain valuable insights into their sales performance, identify areas of strength and weakness, and make data-driven decisions to improve their sales processes.


* Sample of Data [link of the sample](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final?resource=download&select=Sample+-+Superstore.csv).


* Metadata
  * Row ID, Order ID, Order Date, Ship Date, Ship Mode, Customer ID, Customer Name, 
    Segment, Country, City, State, Postal Code, Region, Product ID, Category, 
        Sub-Category, Product Name, Sales, Quantity, Discount, Profit
  
---


## Table of Contents


1. [Introduction](#introduction)
2. [Getting Started](#getting-started)
   - [Prerequisites](#prerequisites)
   - [Installation](#installation)
3. [Project Structure](#project-structure)
   - [High-level structure of dbt project, including folders and files](#high-level-structure-of-dbt-project-including-folders-and-files)
   - [Star Schema Structure used](#star-schema-structure-used)
4. [Configuration](#configuration)
   - [Sources](#sources)
   - [Dbt project yml](#dbt-project-yml)
5. [Models](#models)
   - [Stage model](#stage-model)
   - [Dimension models](#dimension-models)
      - [Product dimension](#Product-dimension)
      - [Customer dimension](#Customer-dimension)
      - [Calendar date dimension](#Calendar-date-dimension)
      - [Location dimension](#location-dimension)
   - [Fact model](#fact-model)
      - [Orders fact](#Orders-fact)
6. [Queries](#queries)
   - [Query 1: top quantity sold products per year](#query-1-top-quantity-sold-products-per-year)
   - [Query 2: total sales for each category](#query-2-total-sales-for-each-category)
   - [Query 3: customers made many orders in the same day](#query-3-customers-made-many-orders-in-the-same-day)
   - [Query 4: top negative profit per city and states](#query-4-top-negative-profit-per-city-and-states)
   - [Query 5: monthly sales growth percentage](#query-5-monthly-sales-growth-percentage)
   - [Query 6: difference between ship and order dates and the relation with the ship mode](#query-6-difference-between-ship-and-order-dates-and-the-relation-with-the-ship-mode)
   - [Query 7: Average profit margin for each subcategory](#query-7-Average-profit-margin-for-each-subcategory)
7. [Tests](#tests)
8. [Common Issues](#common-issues)
   - [Business Cases](#Business-Cases)
9. [License](#license)

---
## Introduction

Designing a Simple Star Schema using dbt form `Kaggle` Dataset on Snowflake, 
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
* Dbt_ATOS_Train/
  * documentation/
    * documentation_file.md

  * models/
    * DWH/
      * Schema_superstore/
        * Dimensions/
          * _dimensions__models.yml
          * dim_calendar_date.sql
          * dim_customer.sql
          * dim_location.sql
          * dim_product.sql
        * Facts/
          * _facts__models.yml
          * fact_orders.sql
    * marts/ 
      * Schema_superstore/
        * Queries/
          * _queries__models.yml
          * top_quantity_sold_products_per_year.sql
          * total_sales_per_category.sql
          * customers_made_many_orders_same_day.sql
          * top_negative_profit_city_and_states.sql
          * calculate_monthly_sales_growth_percentage.sql
          * ship_order_diff_dates_relation_with_ship_mode.sql
          * avg_profit_margin_per_subcategory.sql
    * staging/
      * Schema_superstore/
        * _schema_superstore__models.yml
        * _schema_superstore__sources.yml
        * stg_schema_superstore__superstore_sample.sql
  
  * dbt_project.yml



### Star Schema Structure used.

Using website [dbdiagram](https://dbdiagram.io/).

![SuperStore System](https://github.com/mohamedelsheikh20/Dbt_ATOS_Train/assets/65075222/c20ad36d-350a-478a-bb1e-c6ffe7b38fcf)

As the image shows there are `Fact table` with four `Dimension tables`:
1. Orders Fact Table:
   1. Contain all foreign keys `FKs` to connect all dimensions.
   2. Contain all measures to be calculated later.
   3. Contain degenerated dimensions which have no table to add in.

  
2. Customer dimension:
   1. Contain all information about each customer like:
      1. Customer ID.
      2. Name.
      3. Segment.
      4. Sub Category.

3. Product dimension:
   1. Contain all information about the product like:
      1. Product ID.
      2. Name.
      3. Category.
      4. Sub Category.


4. Calendar date dimension:
   1. Dimension that holds all dates in the data set.
   

5. Location dimension:
   1. I created a new dimension for the location of the customer because I found out that  
      some customers have many locations for each one. 
   2. Info about where is exactly the customer live and contain:
      1. Country.
      2. City.
      3. State.
      4. Postal code.
      5. Region.



---
## Configuration

* All config will be found in `yml` files.
* Each `yml` will have info, config and tests about the folder that holds it.

### Sources
#### path: models/staging/schema_superstore/_schema_superstore__sources.yml
#### Description:
* Name the main used schema `schema_superstore`, database `pc_dbt_db` and table `superstore_sample` used in snowflake.
* Add description to the schema and the used table.


### Dbt project yml
#### path: dbt_project.yml
#### Description:
* Change the materialized of the marts folder into table.
---
## Models
### Stage model:
(stage to create replica of the data).
#### path: models/staging/schema_superstore/stg_schema_superstore__superstore_sample.sql
#### Description:
* Select the main table data from `_schema_superstore__sources.yml` in the same path.
* The dataset selected is a cleaned data and there is no cleaning to be done.
* Only I will take a replica from the data.

### Dimension models:
* All dimensions take the same source from the only exist stage model.
* All Dimensions config and tests is in the `yml` file in the same path.
### Product dimension
#### path: models/DWH/Schema_superstore/Dimensions/dim_product.sql
#### Description:
* I found repeated IDs with different name (32 rows of 1862), 
    as I don't know the business use case, so I will keep one name of them using `max`.

* Using distinct product columns with the max value:
  * product_id.
  * max: product_name.
  * max: category.
  * max: sub_category.

* Using the exist primary key `product_id`.



### Customer dimension
#### path: models/DWH/Schema_superstore/Dimensions/dim_customer.sql
#### Description:
* Using distinct customer columns:
  * customer_id.
  * customer_name.
  * segment.
* Using the exist primary key `customer_id`.


### Calendar date dimension
#### path: models/DWH/Schema_superstore/Dimensions/dim_calendar_date.sql
#### Description:
* Using distinct columns of any date:
  * order_date.
  * ship_date.
* Add primary key:
  * Using main date and convert it into the format `YYYYMMDD`.
* Union all dates together to avoid any duplications.


### Location dimension
#### path: models/DWH/Schema_superstore/Dimensions/dim_location.sql
#### Description:
* Using related distinct columns:
  * country.
  * city.
  * state.
  * postal_code.
  * region.
* Add primary key generated using the row number.

### Fact model:
* Fact table model which use the previous dimension tables to join the primary
    key of them as a foreign key in fact table, also use the replica of data in the stage.

* All Facts config and tests is in the `yml` file in the same path.

### Orders fact
#### path: models/DWH/schema_superstore/Facts/fact_orders.sql
#### Description:
1. Main fact measures columns:
   * sales, quantity, discount, profit.

2. Calendar foreign keys:
   1. Merge the calendar date IDs with the fact table as a foreign keys using left join.

3. Degenerate Dimension columns:
   1. ship_mode.

4. Foreign keys from the dimensions:
   1. Using left join to get all the fact rows.
   2. Join is on all distinct columns considered in the mentioned dimension.
---
## Queries



### Query 1: top quantity sold products per year

#### path: models/marts/schema_superstore/Queries/top_quantity_sold_products_per_year.sql

#### Description:

Query to calculate the top sold product for each year.


- Join fact and date dimension using `order_date_id`.

- Group by `date_year` and `product_id` to get each product sum quantity per each year then rank this sum of quantity
  in descending order.

- Finally, take the first rank of each year then group the id of the products to get the info about each product.


### Query 2: total sales for each category

#### path: models/marts/schema_superstore/Queries/total_sales_per_category.sql

#### Description:

Query to find the total sales for each category.


- Join fact and date dimension using `product_id`.

- Get the sum of sales for each `category`.

- Finally, this query can be done for:
  - product (category, sub_category).
  - customer (segment, name).
  - location (any of the location fields).


### Query 3: customers made many orders in the same day

#### path: models/marts/schema_superstore/Queries/customers_made_many_orders_same_day.sql

#### Description:

Query to get customers who made more than one order in the same day.


- Group by `customer_id` and `order_date_id` to get all customers with all dates where `distinct order_id` is more than one order per customer per day.


- Get the data using joining the calendar date and customer dimensions to get the:
  - customer name.
  - calendar dates (year, month and day).


### Query 4: top negative profit per city and states

#### path: models/marts/schema_superstore/Queries/top_negative_profit_city_and_states.sql

#### Description:

Query to get top city and states who make the profit be negative.

- Get the location with the sum of profit less than 0 `negative profits`.

- Group by `location_dim` to get the city and state of the negative profit.


### Query 5: monthly sales growth percentage

#### path: models/marts/schema_superstore/Queries/calculate_monthly_sales_growth_percentage.sql

#### Description:

Query to calculate the monthly sales growth percentage

- Depending on the main equation of the monthly sales growth percentage:
  - ((this month - previous month) / previous month) * 100

- Get the sales per each month in each year.
- Get the sales per each previous month using window function and (lag).
- Do the equation in generated columns.
- Round the output and add `%` to it, so it will be readable.


### Query 6: difference between ship and order dates and the relation with the ship mode

#### path: models/marts/schema_superstore/Queries/ship_order_diff_dates_relation_with_ship_mode.sql

#### Description:

Query to get the relation between ship mode and min-max days of shipping.

- Depending on the difference of order and ship dates, I will get the difference in days.
- Get the min max number of days with each `dd_ship_mode`.

- I let all measures, so I can get any relationship I want with measures.



### Query 7: Average profit margin for each subcategory

#### path: models/marts/schema_superstore/Queries/avg_profit_margin_per_subcategory.sql

#### Description:

Query to find the average profit margin for each product sub-category.

- Using the equation `(profit / sales)` to get the profit margin for the products sub categories.
- Get the average of the output column.

- Display the data with the specific sub category.

---
## Tests

All tests are considered in the `yml` files.
* dimensions_models: unique and not null tests.
* facts_models: all the relationships between (FKs and PKs).

---
## Common Issues
### Business Cases
1. For some users there are many locations for each user.
2. There are some products which have the same ID but the other data are different like name.

---
## License
* MIT License.
* This is just a practice.
