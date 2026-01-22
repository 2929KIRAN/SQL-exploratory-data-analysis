--Advanced Data analytics ( Answer Business Questions)
use DataWarehouse;
--1.Change over time (Analyse how a measure evolve over time)
/* Helps track trends and Identify seasonality in our data
 [Measure] By [Date Dimension]
Eg: Total by Year, Average cost by Month
*/

--a. Analyse sales performance over time

select * from gold.fact_sales;

select year(order_date) as order_year, sum (sales_amount) as total_amount,
		COUNT(distinct customer_key) as total_customers,
		SUM(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by year(order_date)
order by year(order_date); --Yearly breakdown

select month(order_date) as order_month, SUM(sales_amount) as total_amount,
		COUNT(distinct customer_key) as total_customers,
		SUM(quantity) as total_quantity
from gold.fact_sales
where order_date is not NuLL
group by month(order_date)
order by month(order_date);-- Monthly Breakdown over the years

select YEAR(order_date) as order_year,
        month(order_date) as order_month, SUM(sales_amount) as total_amount,
		COUNT(distinct customer_key) as total_customers,
		SUM(quantity) as total_quantity
from gold.fact_sales
where order_date is not NuLL
group by YEAR(order_date) ,month(order_date)
order by YEAR(order_date) ,month(order_date);-- Each year and month breakdown


--2.Cumulative Analysis
/* Aggregate the data progressively over time
  -Helps to understand weather our business is growing or declining 
  =[Cumulative Measure ] By  [Date Dimension]
  eg: Running total sales By Year
      Moving Avg of sales By month
*/ 

--a. Caculate the total sales per month and the running total of sales over time
select 
	order_date,
	total_sales,
	SUM(total_sales) over (partition by order_date order by order_date) as running_total_sales,
	AVG(avg_price) over (partition by order_date order by order_date) as moving_avg_price
FROM (
select 
	DATETRUNC(month, order_date) as order_date,
	SUM(sales_amount) as total_sales,
	AVG(price) as avg_price
FROM gold.fact_sales 
where order_date is not null
group by DATETRUNC(month, order_date)
)t


--3.Performance Analysis (Comparing the current value to a target value)
/* Helps measure success and compares performance 
	Current[Measure]- Target[Measure]
	current sales - Average sales 
    current year sales - Previous year sales
	current sales- lowest sales
*/

--a. Analyse the yearly performance of products by comparing each product's sales to both its 
-- average sales performance and the previous year's sales.

select * from gold.dim_product;

with yearly_product_sales as (
select YEAR(f.order_date) as order_year,
		d.product_name,
		SUM(f.sales_amount) as current_sales
from gold.fact_sales f
left join gold.dim_product d
on d.product_key=f.product_key
where order_date is not null
group by YEAR(f.order_date), d.product_name
)

select order_year,
	   product_name,	
	   current_sales,
	   AVG(current_sales) over (partition by product_name ) as avg_sales,
	   current_sales-  AVG(current_sales) over (partition by product_name ) as diff_avg,
	   case when current_sales-  AVG(current_sales) over (partition by product_name )>0 then 'Above Avg'
		    when current_sales-  AVG(current_sales) over (partition by product_name ) <0 then 'Below Avg'
			Else 'Avg'
		End as avg_change,
	  LAG(current_sales) over (partition by product_name order by order_year) py_sales,
	  current_sales-LAG(current_sales) over (partition by product_name order by order_year) as diff_py,
	  case when current_sales-LAG(current_sales) over (partition by product_name order by order_year) >0 then 'Increase'
	       when current_sales-LAG(current_sales) over (partition by product_name order by order_year) <0 then 'Decrease'
		   Else 'No change'
	End as py_change
from yearly_product_sales
order by product_name, order_year

--4. Part-to-whole (Proportion Analysis)
/* Analyse how an individual part is performing compared to the overall, allowing us to understand which cateogry
   has the biggest impact on the business 
   ([Measure ]/Total [Measure]) *100 By [Dimension] 
   --eg: (sales/Total sales) *100 By category
   (Quantity / Total Quantity ) * 100 By country
*/

--a.Which cateogry contribute the most to overall sales 

select * from gold.dim_product;
select * from gold.fact_sales;

with category_sales AS (
select category, SUM(sales_amount) as overall_sales
from gold.fact_sales f
left join gold.dim_product d
on d.product_key=f.product_key
group by category
) 

select category ,
		overall_sales,
		SUM(overall_sales) over () as total_sales,
		concat(round((cast(overall_sales as float)/SUM(overall_sales) over () )* 100, 2), '%' ) as percentage_sales
from category_sales
order by overall_sales desc;

--Which country contrubute the most to the overall revenue

with country_category as 
(
select d.country, SUM(f.sales_amount)as total_Sales
from gold.fact_sales f
left join gold.dim_customers d
on d.customer_key=f.customer_key
--where country <> 'N/A'
group by d.country
)
select 
	country,
	total_sales,
	SUM(total_sales ) over() as overall_sales,
	concat(round((cast (total_Sales as float)/ SUM(total_sales ) over())*100,2), '%' ) as percentage_sales
from country_category
order by total_Sales desc;

--5.Data Segmentation 
/* Group the data based on a specific range. 
   Helps understand the correlation between two measure 
  [Measure] by [measure]
  eg: total products by sales range
      total cusotomer by Age
*/

--a. segment products into cost ranges and count how many products fall into each segment
with cost_segment as 
(
select product_key, 
	   product_name,
	   product_cost,
	   case when product_cost < 100 then '0-100'
			when product_cost between 100 and 500 then '100-500'
			when product_cost between 500 and 1000 then '500-1000'
			else 'Above 1000'
		end as cost_range
from gold.dim_product
) 
select cost_range,
		COUNT(product_key) as total_products
from cost_segment
group by cost_range;


/*Group customers into three segments based on their spending behavior:
VIP: Customers with at least 12 months of history and spending more than €5,000.
Regular: Customers with at least 12 months of history but spending €5,000 or less.
New: Customers with a lifespan less than 12 months.
And find the total number of customers by each group
*/
with lifespan_segment as (
select d.customer_key, 
	   sum(f.sales_amount) as total_spending,
	   MIN(order_date) as first_order,
	   MAX(order_date) as last_order,
	   DATEDIFF(month, MIN(order_date), MAX(order_date)) as lifespan
from gold.fact_sales f
left join gold.dim_customers d
on d.customer_key= f.customer_key
group by d.customer_key
)
select segment,
		COUNT (customer_key) as total_customers
from (
		select 
		customer_key,
		case when lifespan >= 12 and total_spending > 5000 then 'VIP'
			 when lifespan >=12 and total_spending <= 5000 then 'Regular'
			 else 'New'
		End as segment
from lifespan_segment ) t 
group by segment
order by total_customers desc;




