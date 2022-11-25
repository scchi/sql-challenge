-- A. Data Cleansing Steps

-- In a single query, perform the following operations and generate a new table in the data_mart schema named clean_weekly_sales:
--
-- - Convert the week_date to a DATE format
-- - Add a week_number as the second column for each week_date value, for example any value from the 1st of January to 7th of January will be 1, 8th to 14th will be 2 etc
-- - Add a month_number with the calendar month for each week_date value as the 3rd column
-- - Add a calendar_year column as the 4th column containing either 2018, 2019 or 2020 values
-- - Add a new column called age_band after the original segment column using the following mapping on the number inside the segment value
-- - Add a new demographic column using the following mapping for the first letter in the segment values:
-- - Ensure all null string values with an "unknown" string value in the original segment column as well as the new age_band and demographic columns
-- - Generate a new avg_transaction column as the sales value divided by transactions rounded to 2 decimal places for each record

DROP TABLE IF EXISTS data_mart.clean_weekly_sales;
CREATE TABLE data_mart.clean_weekly_sales AS
SELECT
  TO_DATE(week_date, 'DD/MM/YY') AS week_date,
  DATE_PART('week', TO_DATE(week_date, 'DD/MM/YY')) AS week_number,
  DATE_PART('mon', TO_DATE(week_date, 'DD/MM/YY')) as month_number,
  DATE_PART('year', TO_DATE(week_date, 'DD/MM/YY')) as calendar_year,
  region,
  platform,
  CASE
    WHEN segment = 'null' THEN 'Unknown'
    ELSE segment
  END AS segment,
  CASE
    WHEN RIGHT(segment, 1) = '1' THEN 'Young Adults'
    WHEN RIGHT(segment, 1) = '2' THEN 'Middle Aged'
    WHEN RIGHT(segment, 1) IN ('3', '4') THEN 'Retirees'
    ELSE 'Unknown'
  END AS age_band,
  CASE
    WHEN LEFT(segment, 1) = 'C' THEN 'Couples'
    WHEN LEFT(segment, 1) = 'F' THEN 'Families'
    ELSE 'Unknown'
  END AS demographic,
  customer_type,
  transactions,
  sales,
  ROUND(sales / transactions, 2) AS avg_transaction
FROM
  data_mart.weekly_sales;

-- B. Data Exploration

-- 1. What day of the week is used for each week_date value?

SELECT
  TO_CHAR(week_date, 'Day')
FROM
  data_mart.clean_weekly_sales
LIMIT
  1

-- 2. What range of week numbers are missing from the dataset?

WITH all_week_numbers AS (
  SELECT
    GENERATE_SERIES(1, 52) as week_number
),
distinct_week_numbers AS (
  SELECT
    DISTINCT week_number as week_number
  FROM
    data_mart.clean_weekly_sales
)
SELECT
  a.week_number
FROM
  all_week_numbers a
WHERE
  NOT EXISTS (
    SELECT
      1
    FROM
      distinct_week_numbers d
    WHERE
      a.week_number = d.week_number
  )

-- 3. How many total transactions were there for each year in the dataset?

SELECT
  calendar_year,
  SUM(transactions)
FROM
  data_mart.clean_weekly_sales
GROUP BY
  calendar_year
ORDER BY
  calendar_year

-- 4. What is the total sales for each region for each month?

SELECT
  region,
  month_number,
  SUM(sales)
FROM
  data_mart.clean_weekly_sales
GROUP BY
  region, month_number
ORDER BY
  region, month_number

-- 5. What is the total count of transactions for each platform

SELECT
  platform,
  COUNT(transactions)
FROM
  data_mart.clean_weekly_sales
GROUP BY
  platform
ORDER BY
  platform

-- 6. What is the percentage of sales for Retail vs Shopify for each month?

WITH monthly_platform_sales_cte AS (
  SELECT
    month_number,
    platform,
    SUM(sales) AS monthly_sales
  FROM
    data_mart.clean_weekly_sales
  GROUP BY
    month_number,
    platform
)
SELECT
  month_number,
  ROUND(
    100 * SUM(
      CASE
        WHEN platform = 'Retail' THEN monthly_sales
        ELSE NULL
      END
    ) / SUM(monthly_sales) :: NUMERIC,
    2
  ) AS retail_percentage,
  ROUND(
    100 * SUM(
      CASE
        WHEN platform = 'Shopify' THEN monthly_sales
        ELSE NULL
      END
    ) / SUM(monthly_sales) :: NUMERIC,
    2
  ) AS shopify_percentage
FROM
  monthly_platform_sales_cte
GROUP BY
  month_number
ORDER BY
  month_number

-- 7. What is the percentage of sales by demographic for each year in the dataset?

WITH yearly_demographic_sales_cte AS (
  SELECT
    calendar_year,
    demographic,
    SUM(sales) AS yearly_sales
  FROM
    data_mart.clean_weekly_sales
  GROUP BY
    calendar_year,
    demographic
  ORDER BY
    calendar_year,
    demographic
)
SELECT
  calendar_year,
  ROUND(
    100 * SUM(
      CASE
        WHEN demographic = 'Couples' THEN yearly_sales
        ELSE NULL
      END
    ) / SUM(yearly_sales),
    2
  ) as couples_percentage,
    ROUND(
    100 * SUM(
      CASE
        WHEN demographic = 'Families' THEN yearly_sales
        ELSE NULL
      END
    ) / SUM(yearly_sales),
    2
  ) as families_percentage,
    ROUND(
    100 * SUM(
      CASE
        WHEN demographic = 'Unknown' THEN yearly_sales
        ELSE NULL
      END
    ) / SUM(yearly_sales),
    2
  ) as unknown_percentage
FROM
  yearly_demographic_sales_cte
GROUP BY
  calendar_year

-- 8. Which age_band and demographic values contribute the most to Retail sales?

SELECT
  age_band,
  SUM(sales) AS sales_contribution
FROM
  data_mart.clean_weekly_sales
WHERE
  platform = 'Retail'
GROUP BY
  age_band
ORDER BY
  sales_contribution DESC

SELECT
  demographic,
  SUM(sales) AS sales_contribution
FROM
  data_mart.clean_weekly_sales
WHERE
  platform = 'Retail'
GROUP BY
  demographic
ORDER BY
  sales_contribution DESC

SELECT
  age_band,
  demographic,
  SUM(sales) AS sales_contribution
FROM
  data_mart.clean_weekly_sales
WHERE
  platform = 'Retail'
GROUP BY
  demographic, age_band
ORDER BY
  sales_contribution DESC

-- 9. Can we use the avg_transaction column to find the average transaction size for each year for Retail vs Shopify? If not - how would you calculate it instead?

SELECT
  calendar_year,
  ROUND(
    SUM(
      CASE
        WHEN platform = 'Retail' THEN sales
        ELSE NULL
      END
    ) / SUM(
      CASE
        WHEN platform = 'Retail' THEN transactions
        ELSE NULL
      END
    ),
    0
  ) as retail_average_transactions,
    ROUND(
    SUM(
      CASE
        WHEN platform = 'Shopify' THEN sales
        ELSE NULL
      END
    ) / SUM(
      CASE
        WHEN platform = 'Shopify' THEN transactions
        ELSE NULL
      END
    ),
    0
  ) as shopify_average_transactions
FROM
  data_mart.clean_weekly_sales
GROUP BY
  calendar_year
ORDER BY
  calendar_year

--  C. Before & After Analysis

-- Taking the week_date value of 2020-06-15 as the baseline week where the Data Mart sustainable packaging changes came into effect.
--
-- We would include all week_date values for 2020-06-15 as the start of the period after the change and the previous week_date values would be before
--
-- Using this analysis approach - answer the following questions:
--
-- 1. What is the total sales for the 4 weeks before and after 2020-06-15? What is the growth or reduction rate in actual values and percentage of sales?
-- 2. What about the entire 12 weeks before and after?
-- 3. How do the sale metrics for these 2 periods before and after compare with the previous years in 2018 and 2019?

-- D. Bonus Question

-- Which areas of the business have the highest negative impact in sales metrics performance in 2020 for the 12 week before and after period?

-- - region
-- - platform
-- - age_band
-- - demographic
-- - customer_type

-- Do you have any further recommendations for Danny’s team at Data Mart or any interesting insights based off this analysis?
