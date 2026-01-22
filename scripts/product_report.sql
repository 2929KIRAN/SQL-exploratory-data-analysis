/*
Product Report
===
Purpose:
- This report consolidates key product metrics and behaviors.
Highlights:
		1. Gathers essential fields such as product name, category, subcategory, and cost.
		2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
		3. Aggregates product-level metrics:
			-total orders
			-total sales
			-total quantity sold
			-total customers (unique)
			-lifespan (in months)
4. Calculates valuable KPIs:
		recency (months since last sale)
		average order revenue (AOR)
		average monthly revenue
*/
use DataWarehouse;
create view  gold.report_products as 
with base_query as 
/* -------------------------
  1.Base Query: Retrieves core column from table 
  -------------------------- */
(
select f.order_number,
		f.product_key,
		f.order_date,
		f.sales_amount,
		f.quantity,
		f.customer_key,
		d.product_name,
		d.category,
		d.subcategory,
		d.product_cost
from gold.fact_sales f
left join gold.dim_product d
on d.product_key= f.product_key
where order_date is not null

), product_aggregation as (
/* -------------------------
  1.Product Aggregations: Summarize key metrices at the Product level 
  -------------------------- */
select 
	   product_key,
	    product_name,
	   category,
	   subcategory,
	   product_cost,
	   COUNT(DISTINCT order_number) as total_orders,
	   MAX(order_date) as last_sale_date,
	  DATEDIFF( month, MIN(order_date) , MAX(order_date)) as lifespan,
	   SUM(sales_amount) as total_sales,
	   sum(quantity) as total_quantity,
	   COUNT(distinct customer_key) as total_customers,
	   round(AVG(cast(sales_amount AS float) / nullif(quantity,0)), 1)  as avg_selling_price
	   --price,
	   
from base_query
group by product_key, product_name, category, subcategory, product_cost
)
/* ----------------------------
3. Final Query: Combining all product results into one output
   ---------------------------- */
select 
		product_key,
	    product_name,
	   category,
	   subcategory,
	   product_cost,
	   total_orders,
	   last_sale_date,
	   DATEDIFF(month, last_sale_date, GETDATE()) as recency,
	   case when total_sales > 50000 then 'High Performer'
	        when total_sales >= 10000 then 'Mid-Range'
		ELSE 'Low performer'
		END as product_segment,
	   lifespan,
	   total_sales,
	    total_quantity,
	   total_customers,
	   avg_selling_price,
	   -- Average order revenue (AOR)
	   case when total_orders=0 then 0
	        else total_sales/total_orders 
		END as Avg_order_revenue,
	  -- Average Monthly Revenue 
	  case when lifespan=0 then total_sales
	       else total_sales/lifespan 
	  END as avg_monthly_revenue
from product_aggregation

select * from gold.report_products;
