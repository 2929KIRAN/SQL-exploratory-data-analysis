/*
Customer Report
=
Purpose:
- This report consolidates key customer metrics and behaviors
Highlights:
	1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
	3. Aggregates customer-level metrics:
		-total orders
		-total sales
		-total quantity purchased
		-total products
	`   -lifespan (in months)
	4. Calculates valuable KPIs:
		-recency (months since last order)
		-average order value
		-average monthly spend
*/
USE DataWarehouse;

create view gold.report_customers as 
with base_query as (
/* -------------------------
  1.Base Query: Retrieves core column from table 
  -------------------------- */
select 
		f.order_date,
		f.order_number,
		f.product_key,
		f.sales_amount,
		f.quantity,
		d.customer_key,
		d.customer_number,
		CONCAT(d.first_name , ' ', d.last_name) as customer_name,
		DATEDIFF( year, d.birthdate, GETDATE()) as age
from gold.fact_sales f
left join gold.dim_customers d
on d.customer_key= f.customer_key
where order_date is not NULL
) 
, customer_aggregation as (
/* -------------------------
  1.Customer Aggregations: Summarize key metrices at the customer level 
  -------------------------- */
select 
	customer_key,
	customer_number,
	customer_name,
	age,
	COUNT(Distinct order_number) as total_orders,
	SUM(sales_amount) as total_Sales,
	SUM(quantity) as total_quantity,
	COUNT(Distinct product_key) as total_products,
	MAX(order_date) as last_order_date,
	DATEDIFF( month, MIN(order_date) , MAX(order_date)) as lifespan
 from base_query
 group by customer_key, customer_name, customer_number, age

 )

 select 
	customer_key,
	customer_number,
	customer_name,
	age,
	case when age <20 then 'Under 20'
		 when age between 20 and 29 then '20-29'
		 when age between 30 and 39 then '30-39'
		 when age between 40 and 49 then '40-49'
     ELSE 'Above 50'
	 End as age_group,
	case when lifespan>=12 and total_Sales >=5000 then 'VIP'
		 when lifespan >=12 and total_Sales <=5000 then 'Regular'
		 else 'New'
	End as customer_segment,
	last_order_date,
	DATEDIFF( month, last_order_date, getdate()) as recency,
	total_orders,
	total_Sales,
	total_quantity,
	total_products,
	lifespan,
	--Computing average order value
	case when total_Sales = 0 then 0
	     else total_Sales/ total_orders 
	End	as avg_order_value,
	-- Compute average monthly spend
	case when total_Sales =0 then 0
	     when lifespan =0 then total_Sales
	     else total_Sales/lifespan 
	End as avg_monthly_spend
from customer_aggregation


select * from gold.report_customers
