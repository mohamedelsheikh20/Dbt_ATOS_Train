# Update Queries
Here you will find some use cases and SQL code. 
- Analyze the use cases and find more efficient code to enhance the logic.
- Include the improved SQL code for your choice, along with an explanation of why it is better.
---

## Content
1. [Example 1](#Example-1-Identify-the-salesperson-with-the-highest-sales-for-each-quarter-of-the-year-for-the-past-two-years)
   - [ex 1 old sql.](#ex-1-old-SQL-code)
   - [ex 1  new sql.](#ex-1-new-SQL-code)
   - [ex 1  Updates.](#Example-1-updates)

2. [Example 2](#Example-2-Identifying-the-top-5-salespeople-based-on-total-sales-for-the-year)
   - [ex 2 old sql.](#ex-2-old-SQL-code)
   - [ex 2  new sql.](#ex-2-new-SQL-code)
   - [ex 2  Updates.](#Example-2-updates)

3. [Example 3](#Example-3-Return-a-list-of-all-the-employees-in-those-departments)
   - [ex 3 old sql.](#ex-3-old-SQL-code)
   - [ex 3  new sql.](#ex-3-new-SQL-code)
   - [ex 3  Updates.](#Example-3-updates)

4. [Example 4](#Example-4-Find-Duplicate-Rows)
   - [ex 4 old sql.](#ex-4-old-SQL-code)
   - [ex 4  new sql.](#ex-4-new-SQL-code)
   - [ex 4  Updates.](#Example-4-updates)

5. [Example 5](#Example-5-Compute-a-Year-Over-Year-Difference)
   - [ex 5 old sql.](#ex-5-old-SQL-code)
   - [ex 5  new sql.](#ex-5-new-SQL-code)
   - [ex 5  Updates.](#Example-5-updates)

6. [Example 6](#Example-6-Return-the-MAX-sales-and-2nd-max-sales)
   - [ex 6 old sql.](#ex-6-old-SQL-code)
   - [ex 6  new sql.](#ex-6-new-SQL-code)
   - [ex 6  Updates.](#Example-6-updates)

7. [Example 7](#Example-7)
   - [ex 7 old sql.](#ex-7-old-SQL-code)
   - [ex 7  new sql.](#ex-7-new-SQL-code)
   - [ex 7  Updates.](#Example-7-updates)
---


## Example 1: Identify the salesperson with the highest sales for each quarter of the year for the past two years.

### Ex 1: old SQL code
```
SELECT
    sp.salesperson_id,
    q.quarter_name,
    SUM(t.order_amount) AS total_sales
FROM sales_transactions AS t
JOIN salespeople AS sp ON t.salesperson_id = sp.salesperson_id
JOIN quarters AS q ON t.transaction_date BETWEEN q.start_date AND q.end_date
GROUP BY
    sp.salesperson_id,
    q.quarter_name
```

### Ex 1: new SQL code
```
-- Identify the salespersons with the highest sales for each quarter of the year for the past two years
all_sales_persons as (
    select
    
        quarters.quarter_name,
        sales_transactions.salesperson_id,
        sum(sales_transactions.order_amount) as total_sales,
        rank() over (
            partition by quarters.quarter_name
            order by sum(sales_transactions.order_amount) desc
        ) as rnk
    
    from sales_transactions
    
    join quarters on 
        sales_transactions.transaction_date between quarters.start_date and quarters.end_date
        
    where
        sales_transactions.transaction_date >= dateadd(Year, -2, getdate())
    
    group by
        quarters.quarter_name,
        sales_transactions.salesperson_id
),

-- get the highest salesperson for each quarter
select

    quarter_name,
    salesperson_id,
    total_sales

from all_sales_persons

where
    rnk = 1
```

### **Example 1: updates**
* lower all statement and improve the structure (spaces, removing leading words like `as X`)
* there is no need to join and group by `salespeople` because we use only the id (in this business case).
* add where statement to get only the past 2 years
* add rank window function to make the highest salesperson = 1 using descending order of `order_amount` sum.
---


## Example 2: Identifying the top 5 salespeople based on total sales for the year.

### Ex 2: old SQL code
```
SELECT
    sp.salesperson_id,
    SUM(t.order_amount) AS total_sales
FROM sales_transactions AS t
JOIN salespeople AS sp ON t.salesperson_id = sp.salesperson_id
GROUP BY
    sp.salesperson_id
ORDER BY total_sales DESC
LIMIT 5
```

### Ex 2: new SQL code
```
select

    sales_transactions.salesperson_id,
    sum(sales_transactions.order_amount) as total_sales

from sales_transactions

where 
    extract(year from sales_transactions.transaction_date) = 2023

group by
    sales_transactions.salesperson_id

order by
    total_sales desc

limit 5
```

### **Example 2: updates**
* lower all statement and improve the structure (spaces, removing leading words like `as X`)
* there is no need to join and group by `salespeople` because we use only the id (in this business case).
* assume that the use case want certain year like `2023`, so I added it into the `where` statement.
---


## Example 3: Return a list of all the employees in those departments.

### Ex 3: old SQL code
```
SELECT 
  first_name,
  last_name
FROM employee e1
WHERE department_id IN (
   SELECT department_id
   FROM department
   WHERE manager_name=‘John Smith’)
```

### Ex 3: new SQL code
```
select 

  employee.first_name,
  employee.last_name

from employee

join department on
    employee.department_id = department.department_id

where 
    department.manager_name=‘John Smith’
```

### **Example 3: updates**
* lower all statement and improve the structure (spaces, removing leading words like `as X`)
* join department instead of making a sub query, with adding the same `where` statement.
---


## Example 4: Find Duplicate Rows

### Ex 4: old SQL code
```
SELECT 
  employee_id,
  last_name,
  first_name,
  dept_id,
  manager_id,
  salary
FROM employee
GROUP BY   
  employee_id,
  last_name,
  first_name,
  dept_id,
  manager_id,
  salary
HAVING COUNT(*) > 1
```

### Ex 4: new SQL code
```
select 

  employee_id,
  last_name,
  first_name,
  dept_id,
  manager_id,
  salary

from employee

group by   
  employee_id,
  last_name,
  first_name,
  dept_id,
  manager_id,
  salary

having count(*) > 1
```

### **Example 4: updates**
* lower all statement and improve the structure (spaces, removing leading words like `as X`)
* there is no updates in main code flow.
---


## Example 5: Compute a Year-Over-Year Difference
-  The main query should return the year, year_amount, revenue_previous_year, Year-Over-Year_diff_value,
        and Year-Over-Year_diff_perc(%) from the year_metrics

- Equations:
  - Year-over-year difference = Value Current Year – Value Last Year
  - Year-over-year percentage `Growth` = ((Value Current Year – Value Last Year) / Value Last Year) x 100

### Ex 5: old SQL code
```
SELECT
    extract(year from day) as year,
    SUM(daily_amount) as year_amount
  FROM sales
  GROUP BY year
```

### Ex 5: new SQL code
```
data_year_amount as (

    select
    
        extract(year from day) as year,
        sum(daily_amount) as year_amount
    
      from sales
      
      group by year

),

previous_year_amount as (

    select
    
        year,
        year_amount,
        lag(year_amount) over (
            order by year
        ) as previous_year_amount
    
    from data_year_amount

)

select
    
    year,
    year_amount,
    (year_amount - previous_year_amount) as Year_Over_Year_diff_value
    ((year_amount - previous_year_amount) / previous_year_amount) * 100 as Year_over_year_percentage


from previous_year_amount
```

### **Example 5: updates**
* lower all statement and improve the structure (spaces, removing leading words like `as X`)
* add CTE to get the amount for each year.
* add CTE with new column which have the value of the previous year.
* depending on the equations of `Year_Over_Year_diff_value` and `Year_over_year_percentage` add new columns.
---


## Example 6: Return the MAX sales and 2nd max sales

### Ex 6: old SQL code
```
SELECT
(SELECT MAX(sales) FROM Sales_orders) max_sales,
(SELECT MAX(sales) FROM Sales_orders
WHERE sales NOT IN (SELECT MAX(sales) FROM Sales_orders )) as 2ND_max_sales;
```

### Ex 6: new SQL code
```
select

    sales

from Sales_orders

order by sales desc
limit 2

```

### **Example 6: updates**
* lower all statement and improve the structure (spaces, removing leading words like `as X`)
* order sales and get the max 2 only.

> depending on business case output we can use or add window function.
---


## Example 7:
- This query will select the `Essns` of all employees who work the same
(project, hours) combination on same project that employee ‘John Smith’ (whose Ssn =‘123456789’) works on.

- from this query we can assume that:
  - project table has : Dnum (department number) - Pnumber (project number).
  - department table has: Dnumber (department number) - mgr_ssn (manager ss number).
  - employee table has: Ssn (ss number) - Fname (first name) - Lname (last name).
  - works_on table has: Essn (employee ss number) - Pno (project number).

### Ex 7: old SQL code
```
SELECT DISTINCT Pnumber
FROM PROJECT
WHERE Pnumber IN  
( SELECT Pnumber
FROM PROJECT, DEPARTMENT, EMPLOYEE
WHERE Dnum=Dnumber AND
Mgr_ssn=Ssn AND Lname=‘Smith’ )
OR
Pnumber IN
( SELECT Pno
FROM WORKS_ON, EMPLOYEE
WHERE Essn=Ssn AND Lname=‘Smith’ )
```

### Ex 7: new SQL code
```
with

-- get the projects where `John Smith` works, where his Ssn =‘123456789’
john_smith_projects as (

   select
   
      project.Pnumber,
      project.Dnum
         
   from project
   
   where
      works_on.Essn = 123456789
   
   join works_on on
      project.Pnumber = works_on.Pno
      
),

-- get all empolyees working on the projects where john smith works
john_smith_colleges as (
   
   select distinct
   
      works_on.Essn as Ssn
   
   from works_on
   
   where Pno in (select Pnumber from john_smith_projects)
),

-- get the department managers of the projects where john smith works
projects_managers as (
   
   select distinct
   
      department.mgr_ssn as Ssn
   
   from john_smith_projects
   
   left join department on
      john_smith_projects.Dnum = department.Dnumber

),

show_all as (

    select
        *
    from john_smith_colleges
    
    union
    
    select
        *
    from projects_managers
    
)

select
    
    employee.Ssn,
    employee.Fname,
    employee.Lname
    
from show_all

join employee on
    employee.Ssn = show_all.Ssn

```

### **Example 7: updates**

* lower all statement and improve the structure (spaces, removing leading words like `as X`).
* as shown above I have assumed the tables and what it contains.
* CTEs:
  1. get the projects where `John Smith` works, where his Ssn = `123456789` because it is a unique number.
  2. get all distinct (unique) employees working on the projects where john smith works.
  3. get the distinct department managers of the projects where john smith works.
  4. union both `employees` and `managers` in one table.
  5. Finally, join with table `employee` to get the `Fname` and `Lname`.
> depending on business case, I assumed this using the old SQL query solution.
---
