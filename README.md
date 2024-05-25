# Retail Orders ETL and Analysis

This repository contains code and insights for extracting, transforming, and loading (ETL) a retail orders dataset, followed by performing data analysis using SQL queries. The dataset is sourced from Kaggle and processed using Python and PostgreSQL.

## Table of Contents
- [Getting Started](#getting-started)
- [ETL Process](#etl-process)
- [Data Analysis](#data-analysis)
- [Queries](#queries)
- [Conclusion](#conclusion)

## Getting Started

### Prerequisites

- Python 3.x
- PostgreSQL
- Kaggle API
- Required Python packages:
  - pandas
  - sqlalchemy
  - kaggle


1. **Install the required Python packages:**

   ```sh
   pip install pandas sqlalchemy kaggle
   ```

2. **Set up Kaggle API:**

   - Place your `kaggle.json` file (download from kaggle website) in `~/.kaggle/`.

## ETL Process

### 1. Import and Authenticate Kaggle API

```python
import kaggle
kaggle.api.authenticate()
```

### 2. Download Dataset

```python
kaggle.api.dataset_download_files('ankitbansal06/retail-orders', unzip=True)
```

### 3. Read Data from File using Pandas

```python
import pandas as pd
df = pd.read_csv('orders.csv')
df.head(20)
```

### 4. Data Cleaning

- Replace void entries (e.g., 'unknown', 'not available') with NULL:

  ```python
  df = pd.read_csv('orders.csv', na_values=['Not Available', 'unknown'])
  ```

- Rename columns to be code-friendly:

  ```python
  df.columns = df.columns.str.lower().str.replace(' ', '_')
  ```

### 5. Data Manipulation

- Add new columns:

  ```python
  df['discount'] = df['list_price'] * df['discount_percent'] / 100
  df['sale_price'] = df['list_price'] - df['discount']
  df['profit'] = round(df['sale_price'] - df['cost_price'], 2)
  ```

- Drop unnecessary columns:

  ```python
  df.drop(columns=['list_price', 'cost_price', 'discount_percent'], inplace=True)
  ```

- Convert `order_date` to datetime:

  ```python
  df['order_date'] = pd.to_datetime(df['order_date'], format='%Y-%m-%d')
  ```

### 6. Connect to Database

```python
import sqlalchemy as sal
engine = sal.create_engine("postgresql://username:password@localhost/database_name")
conn = engine.connect()
```

### 7. Load Data into SQL

```python
df.to_sql('df_orders', con=conn, index=False, if_exists='replace')
```
## Data Analysis

The following SQL queries are used for data analysis, mainly to answer a few primary questions:

### Queries

1. **Top 10 Highest Revenue Generating Products:**

    ```sql
    SELECT product_id, SUM(sale_price)::NUMERIC(10,2) AS revenue
    FROM df_orders
    GROUP BY product_id
    ORDER BY revenue DESC
    LIMIT 10;
    ```

2. **Top 5 Highest Selling Products in Each Region:**

    ```sql
    WITH cte AS (
        SELECT region, product_id, SUM(quantity) AS total_sales
        FROM df_orders
        GROUP BY region, product_id)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY region ORDER BY cte.total_sales DESC) AS rank
        FROM cte)
    WHERE rank < 6;
    ```

3. **Month-over-Month Growth Comparison for 2022 and 2023 Sales:**

    ```sql
    WITH cte AS (
        SELECT EXTRACT(year FROM order_date) AS order_year, EXTRACT(month FROM order_date) AS order_month, SUM(quantity) AS sales
        FROM df_orders
        GROUP BY order_year, order_month
    )
    SELECT order_month, 
           SUM(CASE WHEN order_year = 2022 THEN sales END) AS Year_2022, 
           SUM(CASE WHEN order_year = 2023 THEN sales END) AS Year_2023, 
           (((SUM(CASE WHEN order_year = 2022 THEN sales END)) - (SUM(CASE WHEN order_year = 2023 THEN sales END))) * 100 / (SUM(CASE WHEN order_year = 2022 THEN sales END)))::NUMERIC(10,2) AS percent_change
    FROM cte
    GROUP BY order_month
    ORDER BY order_month;
    ```

4. **Highest Sales Month for Each Category:**

    ```sql
    WITH cte AS (
        SELECT category, CONCAT(EXTRACT(year FROM order_date), EXTRACT(month FROM order_date)) AS year_month, SUM(quantity) AS sales
        FROM df_orders
        GROUP BY category, year_month
    )
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY category ORDER BY sales DESC) AS rank
        FROM cte)
    WHERE rank = 1;
    ```

5. **Sub-Category with Highest Growth by Profit in 2023 Compared to 2022:**

    - **Absolute Change:**

        ```sql
        WITH cte AS (
            SELECT EXTRACT(year FROM order_date) AS order_year, sub_category, SUM(profit) AS profits
            FROM df_orders
            GROUP BY order_year, sub_category
        )
        SELECT sub_category, 
               SUM(CASE WHEN order_year = 2022 THEN profits END)::NUMERIC(20,2) AS Year_2022, 
               SUM(CASE WHEN order_year = 2023 THEN profits END)::NUMERIC(20,2) AS Year_2023, 
               (SUM(CASE WHEN order_year = 2023 THEN profits END) - SUM(CASE WHEN order_year = 2022 THEN profits END))::NUMERIC(20,2) AS profit_change
        FROM cte
        GROUP BY sub_category
        ORDER BY profit_change DESC
        LIMIT 1;
        ```

    - **Percent Change:**

        ```sql
        WITH cte AS (
            SELECT EXTRACT(year FROM order_date) AS order_year, sub_category, SUM(profit) AS profits
            FROM df_orders
            GROUP BY order_year, sub_category
        )
        SELECT sub_category, 
               SUM(CASE WHEN order_year = 2022 THEN profits END)::NUMERIC(20,2) AS Year_2022, 
               SUM(CASE WHEN order_year = 2023 THEN profits END)::NUMERIC(20,2) AS Year_2023, 
               ((SUM(CASE WHEN order_year = 2023 THEN profits END) - SUM(CASE WHEN order_year = 2022 THEN profits END)) * 100 / SUM(CASE WHEN order_year = 2022 THEN profits END))::NUMERIC(20,2) AS profit_change
        FROM cte
        GROUP BY sub_category
        ORDER BY profit_change DESC
        LIMIT 1;
        ```

## Conclusion
This project was a self-learning endeavor aimed at enhancing my understanding of Python and SQL. Through the completion of this project, I have gained significant insights into data analysis, encompassing everything from the fundamental steps of ETL to crafting complex nested queries in SQL. I am now more confident in my ability to handle data analysis tasks using these tools.