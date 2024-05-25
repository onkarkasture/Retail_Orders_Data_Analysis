-- 1. Find top 10 highest revenue generating products
SELECT product_id, SUM(sale_price)::NUMERIC(10,2) AS revenue
FROM orders
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 10;

-- 2. Find the top 5 highest selling products in each region
WITH cte AS (
    SELECT region, product_id, SUM(quantity) AS total_sales
    FROM orders
    GROUP BY region, product_id)
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY region ORDER BY cte.total_sales DESC) AS rank
    FROM cte)
WHERE rank<6


-- 3. Find the month over month growth comparison for 2022 and 2023 sales E.G. Jan 2022 vs Jan 2023
WITH cte AS (
    SELECT EXTRACT(year FROM orders.order_date) AS order_year,EXTRACT(month FROM order_date) AS order_month, SUM(quantity) AS sales
    FROM orders
    GROUP BY order_year, order_month
)
SELECT order_month, SUM(CASE WHEN order_year=2022 THEN sales END) AS Year_2022, SUM(CASE WHEN order_year=2023 THEN sales END) AS Year_2023, (((SUM(CASE WHEN order_year=2022 THEN sales END))-(SUM(CASE WHEN order_year=2023 THEN sales END)))*100/(SUM(CASE WHEN order_year=2022 THEN sales END)))::NUMERIC(10,2) AS percent_change
FROM cte
GROUP BY order_month
ORDER BY order_month;


-- 4. For each category which month had highest sales
WITH cte AS (
    SELECT category, CONCAT(EXTRACT(year from order_date),EXTRACT(month from order_date)) AS year_month, SUM(quantity) AS sales
    FROM orders
    GROUP BY category, year_month
)
SELECT *
FROM (
SELECT *, ROW_NUMBER() OVER(PARTITION BY category ORDER BY sales DESC) AS rank
FROM cte)
WHERE rank=1;

-- 5. Which Sub_Category had highest growth by profit in 2023 compared to 2022
WITH cte AS (
    SELECT EXTRACT(year FROM orders.order_date) AS order_year,sub_category, SUM(profit) AS profits
    FROM orders
    GROUP BY order_year, sub_category
)
SELECT sub_category, SUM(CASE WHEN order_year=2022 THEN profits END)::NUMERIC(20,2) AS Year_2022, SUM(CASE WHEN order_year=2023 THEN profits END)::NUMERIC(20,2) AS Year_2023, (SUM(CASE WHEN order_year=2023 THEN profits END) - SUM(CASE WHEN order_year=2022 THEN profits END))::NUMERIC(20,2) AS profit_change
FROM cte
GROUP BY sub_category
ORDER BY profit_change DESC
LIMIT 1; -- Absolute Change


WITH cte AS (
    SELECT EXTRACT(year FROM orders.order_date) AS order_year,sub_category, SUM(profit) AS profits
    FROM orders
    GROUP BY order_year, sub_category
)
SELECT sub_category, SUM(CASE WHEN order_year=2022 THEN profits END)::NUMERIC(20,2) AS Year_2022, SUM(CASE WHEN order_year=2023 THEN profits END)::NUMERIC(20,2) AS Year_2023, ((SUM(CASE WHEN order_year=2023 THEN profits END) - SUM(CASE WHEN order_year=2022 THEN profits END))*100/SUM(CASE WHEN order_year=2022 THEN profits END))::NUMERIC(20,2) AS profit_change
FROM cte
GROUP BY sub_category
ORDER BY profit_change DESC
LIMIT 1; -- Percent Change