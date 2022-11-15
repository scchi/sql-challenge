-- 1.  How many customers has Foodie-Fi ever had?

SELECT
  COUNT(DISTINCT customer_id) AS total_customers
FROM
  foodie_fi.subscriptions

-- 2.  What is the monthly distribution of  `trial`  plan  `start_date`  values for our dataset - use the start of the month as the group by value

SELECT
  DATE_TRUNC('month', start_date) AS start_month,
  COUNT(*) AS trial_count
FROM
  foodie_fi.subscriptions
WHERE plan_id = 0
GROUP BY start_month
ORDER BY start_month

-- 3.  What plan  `start_date`  values occur after the year 2020 for our dataset? Show the breakdown by count of events for each  `plan_name`

WITH plans_count_post_2020 AS (
  SELECT
    plan_id,
    COUNT(*) as events
  FROM
    foodie_fi.subscriptions
  WHERE
    start_date > '2020-12-31'
  GROUP BY
    plan_id
)
SELECT
  c.plan_id,
  p.plan_name,
  events
FROM
  plans_count_post_2020 c
  INNER JOIN foodie_fi.plans p ON c.plan_id = p.plan_id

-- 4.  What is the customer count and percentage of customers who have churned rounded to 1 decimal place?

SELECT
  SUM(
    CASE
      WHEN plan_id = 4 THEN 1
      ELSE 0
    END
  ) AS churn_count,
  ROUND(
    100 * SUM(
      CASE
        WHEN plan_id = 4 THEN 1
        ELSE 0
      END
    ) / COUNT(DISTINCT customer_id) :: NUMERIC,
    1
  ) AS churn_percentage
FROM
  foodie_fi.subscriptions

-- 5.  How many customers have churned straight after their initial free trial - what percentage is this rounded to the nearest whole number?

WITH event_rank_cte AS (
  SELECT
    customer_id,
    plan_id,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY
        start_date
    ) AS event_rank
  FROM
    foodie_fi.subscriptions
)

SELECT
  SUM(CASE WHEN plan_id = 4 THEN 1 ELSE 0 END) AS churn_customers,
  ROUND(
    100 * SUM(CASE WHEN plan_id = 4 THEN 1 ELSE 0 END) :: numeric /
    COUNT(*), 1
  ) AS percentage
FROM event_rank_cte
WHERE event_rank = 2

-- 6.  What is the number and percentage of customer plans after their initial free trial?

WITH with_previous_plan_cte AS (
  SELECT
    p.plan_id,
    plan_name,
    LAG(plan_name) OVER (
      PARTITION BY customer_id
      ORDER BY
        start_date
    ) AS previous_plan
  FROM
    foodie_fi.subscriptions s
    INNER JOIN foodie_fi.plans p ON s.plan_id = p.plan_id
  ORDER BY
    customer_id,
    start_date
)
SELECT
  plan_id,
  plan_name,
  COUNT(*) AS count_plan,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) over ()) AS percentage
FROM
  with_previous_plan_cte
WHERE
  previous_plan = 'trial'
GROUP BY
  plan_id,
  plan_name

-- 7.  What is the customer count and percentage breakdown of all 5 plan_name values at 2020-12-31?

WITH with_next_plan_cte AS (
  SELECT
    p.plan_id,
    plan_name,
    start_date,
    LEAD(start_date) OVER (
      PARTITION BY customer_id
      ORDER BY
        start_date
    ) AS next_start_date
  FROM
    foodie_fi.subscriptions s
    INNER JOIN foodie_fi.plans p ON s.plan_id = p.plan_id
  ORDER BY
    customer_id,
    start_date
)
SELECT
  plan_id,
  plan_name,
  COUNT(*) customer_count,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS percentage
FROM
  with_next_plan_cte
WHERE
  start_date <= '2020-12-31'
  AND (
    next_start_date > '2020-12-31'
    OR next_start_date IS NULL
  )
GROUP BY
  plan_name,
  plan_id
ORDER BY
  plan_id

-- 8.  How many customers have upgraded to an annual plan in 2020?

WITH year_of_plan_with_previous_cte AS (
  SELECT
    plan_id,
    EXTRACT(
      YEAR
      FROM
        start_date
    ) AS start_year,
    LAG(start_date) OVER (
      PARTITION BY customer_id
      ORDER BY
        start_date
    ) AS previous_plan_start
  FROM
    foodie_fi.subscriptions
)
SELECT
  COUNT(*) AS pro_upgrade_count_2020
FROM
  year_of_plan_with_previous_cte
WHERE
  start_year = 2020
  AND plan_id = 3
  AND previous_plan_start IS NOT NULL

-- 9.  How many days on average does it take for a customer to upgrade to an annual plan from the day they join Foodie-Fi?

WITH annual_plan_cte AS (
  SELECT
    customer_id,
    start_date
  FROM
    foodie_fi.subscriptions
  WHERE
    plan_id = 3
),
trial_cte AS (
  SELECT
    customer_id,
    start_date
  FROM
    foodie_fi.subscriptions
  WHERE
    plan_id = 0
)
SELECT
  ROUND(
    AVG(
      DATE_PART(
        'day',
        a.start_date :: TIMESTAMP - t.start_date :: TIMESTAMP
      )
    )
  ) AS average_days
FROM
  annual_plan_cte a
  INNER JOIN trial_cte t ON a.customer_id = t.customer_id

-- 10.  Can you further breakdown this average value into 30 day periods (i.e. 0-30 days, 31-60 days etc)

WITH annual_plan_cte AS (
  SELECT
    customer_id,
    start_date
  FROM
    foodie_fi.subscriptions
  WHERE
    plan_id = 3
),
trial_cte AS (
  SELECT
    customer_id,
    start_date
  FROM
    foodie_fi.subscriptions
  WHERE
    plan_id = 0
),
thirty_day_period_breakdown_cte as (
  SELECT
    DATE_PART(
      'day',
      a.start_date :: TIMESTAMP - t.start_date :: TIMESTAMP
    ) AS average_day,
    FLOOR(
      DATE_PART(
        'day',
        a.start_date :: TIMESTAMP - t.start_date :: TIMESTAMP
      ) / 30
    ) AS period
  FROM
    annual_plan_cte a
    INNER JOIN trial_cte t ON a.customer_id = t.customer_id
  ORDER BY
    average_day
)
SELECT
  period * 30 || ' - ' || period * 30 + 30 || ' days' as breakdown_period,
  COUNT(*) AS count
FROM
  thirty_day_period_breakdown_cte
GROUP BY
  period
ORDER BY
  period

-- 11.  How many customers downgraded from a pro monthly to a basic monthly plan in 2020?

with with_previous_plan_cte as (
  SELECT
    customer_id,
    plan_id,
    start_date,
    LAG(plan_id) OVER (
      PARTITION BY customer_id
      ORDER BY
        start_date
    ) AS previous_plan_id
  FROM
    foodie_fi.subscriptions
)
select
  count(*)
from
  with_previous_plan_cte
where
  date_part('year', start_date) = 2020
  and previous_plan_id = 2
  and plan_id = 1
