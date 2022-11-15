-- A. Customer Nodes Exploration

-- 1. How many unique nodes are there on the Data Bank system?

WITH node_region_combinations_cte AS (
  SELECT
    distinct node_id,
    region_id
  FROM
    data_bank.customer_nodes
)

SELECT
  COUNT(*)
FROM
  node_region_combinations_cte

-- 2. What is the number of nodes per region?

WITH node_count_per_region_cte AS (
  SELECT
    region_id,
    COUNT(distinct node_id) AS node_count
  FROM
    data_bank.customer_nodes
  GROUP BY
    region_id
)

SELECT
  region_name,
  node_count
FROM
  node_count_per_region_cte n
  INNER JOIN data_bank.regions r ON n.region_id = r.region_id
ORDER BY
  region_name

-- 3. How many customers are allocated to each region?

WITH customer_count_per_region_cte AS (
  SELECT
    region_id,
    COUNT(distinct customer_id) AS customer_count
  FROM
    data_bank.customer_nodes
  GROUP BY
    region_id
)

SELECT
  region_name,
  customer_count
FROM
  customer_count_per_region_cte c
  INNER JOIN data_bank.regions r ON c.region_id = r.region_id
ORDER BY
  region_name

-- 4. How many days on average are customers reallocated to a different node?

DROP TABLE IF EXISTS ranked_customer_nodes;
CREATE TEMP TABLE ranked_customer_nodes AS
SELECT
  customer_id,
  node_id,
  region_id,
  DATE_PART('day', AGE(end_date, start_date)) :: INTEGER AS duration,
  ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY
      start_date
  ) AS rn
FROM
  data_bank.customer_nodes;
WITH RECURSIVE output_table AS (
    SELECT
      customer_id,
      node_id,
      duration,
      rn,
      1 AS run_id
    FROM
      ranked_customer_nodes
    WHERE
      rn = 1
    UNION ALL
    SELECT
      t1.customer_id,
      t2.node_id,
      t2.duration,
      t2.rn,
      CASE
        WHEN t1.node_id != t2.node_id THEN t1.run_id + 1
        ELSE t1.run_id
      END AS run_id
    FROM
      output_table t1
      INNER JOIN ranked_customer_nodes t2 ON t1.rn + 1 = t2.rn
      AND t1.customer_id = t2.customer_id
      AND t2.rn > 1
  ),
  cte_customer_nodes AS (
    SELECT
      customer_id,
      run_id,
      SUM(duration) AS node_duration
    FROM output_table
    GROUP BY
      customer_id,
      run_id
  )

  SELECT
    ROUND(AVG(node_duration)) AS average_node_duration
  FROM cte_customer_nodes;

-- 5. What is the median, 80th and 95th percentile for this same reallocation days metric for each region?

DROP TABLE IF EXISTS ranked_customer_nodes;
CREATE TEMP TABLE ranked_customer_nodes AS
SELECT
  customer_id,
  node_id,
  region_id,
  DATE_PART('day', AGE(end_date, start_date)) :: INTEGER AS duration,
  ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY
      start_date
  ) AS rn
FROM
  data_bank.customer_nodes;
WITH RECURSIVE output_table AS (
    SELECT
      customer_id,
      region_id,
      node_id,
      duration,
      rn,
      1 AS run_id
    FROM
      ranked_customer_nodes
    WHERE
      rn = 1
    UNION ALL
    SELECT
      t1.customer_id,
      t1.region_id,
      t2.node_id,
      t2.duration,
      t2.rn,
      CASE
        WHEN t1.node_id != t2.node_id THEN t1.run_id + 1
        ELSE t1.run_id
      END AS run_id
    FROM
      output_table t1
      INNER JOIN ranked_customer_nodes t2 ON t1.rn + 1 = t2.rn
      AND t1.customer_id = t2.customer_id
      AND t2.rn > 1
  ),
  cte_customer_nodes AS (
    SELECT
      customer_id,
      region_id,
      run_id,
      SUM(duration) AS node_duration
    FROM
      output_table
    GROUP BY
      region_id,
      customer_id,
      run_id
  ),
  duration_percentile AS (
    SELECT
      region_id,
      ROUND(
        PERCENTILE_CONT(0.5) WITHIN GROUP (
          ORDER BY
            node_duration
        )
      ) as median,
      ROUND(
        PERCENTILE_CONT(0.8) WITHIN GROUP (
          ORDER BY
            node_duration
        )
      ) AS pct80,
      ROUND(
        PERCENTILE_CONT(0.95) WITHIN GROUP (
          ORDER BY
            node_duration
        )
      ) AS pct95
    FROM
      cte_customer_nodes
    GROUP BY
      region_id
  )
SELECT
  region_name,
  median,
  pct80,
  pct95
FROM
  duration_percentile t1
  INNER JOIN data_bank.regions t2 ON t1.region_id = t2.region_id

-- B. Customer Transactions

-- 1. What is the unique count and total amount for each transaction type?
-- 2. What is the average total historical deposit counts and amounts for all customers?
-- 3. For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?
-- 4. What is the closing balance for each customer at the end of the month?
-- 5. Comparing the closing balance of a customer’s first month and the closing balance from their second nth, what percentage of customers:
-- - Have a negative first month balance?
-- - Have a positive first month balance?
-- - Increase their opening month’s positive closing balance by more than 5% in the following month?
-- - Reduce their opening month’s positive closing balance by more than 5% in the following month?
-- - Move from a positive balance in the first month to a negative balance in the second month?
