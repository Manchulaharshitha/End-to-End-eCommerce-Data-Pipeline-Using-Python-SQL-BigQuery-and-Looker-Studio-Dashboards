SELECT * 
FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`
LIMIT 10;
--Total Revenue
SELECT 
  SUM(order_value) AS total_revenue
FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`;
--Total Orders
SELECT 
  COUNT(order_id) AS total_orders
FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`;
--Unique Customers
SELECT 
  COUNT(DISTINCT customer_id) AS unique_customers
FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`;
--Total items Sold
SELECT 
  SUM(quantity) AS total_items_sold
FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`;
--Monthly Revenue Trend
SELECT 
  month,
  SUM(order_value) AS monthly_revenue
FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`
GROUP BY month
ORDER BY month;
--Top 10 best selling products
SELECT 
  product_id,
  SUM(quantity) AS total_quantity_sold
FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`
GROUP BY product_id
ORDER BY total_quantity_sold DESC
LIMIT 10;
