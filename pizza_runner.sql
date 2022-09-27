-- Clean data

UPDATE pizza_runner.customer_orders
SET extras = 'none'
where extras = '' or extras = 'null' or extras IS NULL;

UPDATE pizza_runner.customer_orders
SET exclusions = 'none'
where exclusions = '' or exclusions = 'null' or exclusions IS NULL;

update pizza_runner.runner_orders
set cancellation = 'restaurant'
where cancellation = 'Restaurant Cancellation';

update pizza_runner.runner_orders
set cancellation = 'customer'
where cancellation = 'Customer Cancellation';

update pizza_runner.runner_orders
set cancellation = 'n/a'
where cancellation = 'null' or cancellation is null or cancellation = '';

update pizza_runner.runner_orders
set pickup_time = 'n/a'
where pickup_time = 'null';

UPDATE pizza_runner.runner_orders
SET distance = (REGEXP_MATCH(distance, '((\d{2})(\.\d)?)'))[1]
WHERE distance <> 'n/a';

UPDATE pizza_runner.runner_orders
SET duration = (REGEXP_MATCH(duration, '((\d{2})(\.\d)?)'))[1]
WHERE duration <> 'n/a';

-- PIZZA METRICS
-- 1.  How many pizzas were ordered?

SELECT
  COUNT(*)
FROM
  pizza_runner.customer_orders

-- 2.  How many unique customer orders were made?

SELECT
  COUNT(DISTINCT order_id)
FROM
  pizza_runner.customer_orders

-- 3.  How many successful orders were delivered by each runner?

SELECT
  runner_id,
  COUNT(*) as successful_orders
FROM
  pizza_runner.runner_orders
WHERE
  cancellation = 'n/a'
GROUP BY
  runner_id
ORDER BY
  order_count DESC

-- 4.  How many of each type of pizza was delivered?

SELECT
  p.pizza_name,
  COUNT(*) as delivered_pizza_count
FROM
  pizza_runner.customer_orders c
  INNER JOIN pizza_runner.pizza_names p ON p.pizza_id = c.pizza_id
WHERE
  EXISTS (
    SELECT
      1
    FROM
      pizza_runner.runner_orders r
    WHERE
      c.order_id = r.order_id
      AND r.cancellation = 'n/a'
  )
GROUP BY
  p.pizza_name

-- 5.  How many Vegetarian and Meatlovers were ordered by each customer?

SELECT
  DISTINCT customer_id,
  COUNT(*) filter(
    WHERE
      pizza_id = 1
  ) OVER (PARTITION BY customer_id) AS meatlovers,
  COUNT(*) FILTER(
    WHERE
      pizza_id = 2
  ) OVER (PARTITION BY customer_id) AS vegetarian
FROM
  pizza_runner.customer_orders
ORDER BY
  customer_id

-- 6.  What was the maximum number of pizzas delivered in a single order?

SELECT
  order_id,
  COUNT(pizza_id) AS pizza_count
FROM
  pizza_runner.customer_orders c
WHERE
  EXISTS(
    SELECT
      1
    FROM
      pizza_runner.runner_orders r
    WHERE
      c.order_id = r.order_id
      AND cancellation = 'n/a'
  )
GROUP BY
  order_id
ORDER BY
  pizza_count DESC
LIMIT
  1

-- 7.  For each customer, how many delivered pizzas had at least 1 change and how many had no changes?

SELECT
  customer_id,
  SUM(
    CASE
      WHEN extras = 'none'
      AND exclusions = 'none' THEN 1
      ELSE 0
    END
  ) AS no_changes,
  SUM(
    CASE
      WHEN extras <> 'none'
      OR exclusions <> 'none' THEN 1
      ELSE 0
    END
  ) AS at_least_1_change
FROM
  pizza_runner.customer_orders c
WHERE
  EXISTS (
    SELECT
      1
    FROM
      pizza_runner.runner_orders r
    WHERE
      c.order_id = r.order_id
      AND r.cancellation = 'n/a'
  )
GROUP BY
  customer_id
ORDER BY
  customer_id

-- 8.  How many pizzas were delivered that had both exclusions and extras?

SELECT
  SUM(
    CASE
      WHEN c.extras <> 'none'
      AND c.exclusions <> 'none' THEN 1
      ELSE 0
    END
  ) AS had_extras_and_exclusions
FROM
  pizza_runner.customer_orders c
WHERE
  EXISTS (
    SELECT
      1
    FROM
      pizza_runner.runner_orders r
    WHERE
      c.order_id = r.order_id
      AND r.cancellation = 'n/a'
  )

-- 9.  What was the total volume of pizzas ordered for each hour of the day?

WITH order_hour_cte AS (
  SELECT
    EXTRACT(
      'hour'
      FROM
        order_time
    ) AS order_hour
  FROM
    pizza_runner.customer_orders
)
SELECT
  order_hour,
  count(*) AS order_count
FROM
  order_hour_cte
GROUP BY
  order_hour
ORDER BY
  order_hour

-- 10.  What was the volume of orders for each day of the week?

SELECT
TO_CHAR(order_time, 'Day') AS day_of_week,
  COUNT(*) AS order_count
FROM
  pizza_runner.customer_orders
GROUP BY day_of_week

-- RUNNER AND CUSTOMER EXPERIENCE
-- 1.  How many runners signed up for each 1 week period? (i.e. week starts  `2021-01-01`)

SELECT
  (
    DATE_TRUNC('week', registration_date - INTERVAL '4 day') + INTERVAL '4 day'
  ) :: date AS registration_week,
  COUNT(*) AS runners
FROM
  pizza_runner.runners
GROUP BY
  registration_week
ORDER BY
  registration_week

-- 2.  What was the average time in minutes it took for each runner to arrive at the Pizza Runner HQ to pickup the order?

WITH pickup_minutes_cte AS (
  SELECT
    DISTINCT r.order_id,
    DATE_PART(
      'minutes',
      AGE(r.pickup_time :: TIMESTAMP, c.order_time)
    ) :: INTEGER AS pickup_minutes
  FROM
    pizza_runner.runner_orders AS r
    INNER JOIN pizza_runner.customer_orders AS c ON r.order_id = c.order_id
  WHERE
    r.pickup_time != 'n/a'
)
SELECT
  ROUND(AVG(pickup_minutes), 3)
FROM
  pickup_minutes_cte

-- 3.  Is there any relationship between the number of pizzas and how long the order takes to prepare?

WITH prep_time_and_pizza_count_cte AS (
  SELECT
    DISTINCT c.order_id,
    DATE_PART(
      'minutes',
      AGE(r.pickup_time :: TIMESTAMP, c.order_time)
    ) :: INTEGER AS pickup_minutes,
    COUNT(*) OVER (PARTITION BY c.order_id) AS pizza_count
  FROM
    pizza_runner.customer_orders c
    INNER JOIN pizza_runner.runner_orders r ON c.order_id = r.order_id
  WHERE
    cancellation = 'n/a'
)
SELECT
  pizza_count,
  AVG(pickup_minutes) :: INTEGER AS avg_pickup_minutes
FROM
  prep_time_and_pizza_count_cte
GROUP BY
  pizza_count
ORDER BY
  pizza_count

-- 4.  What was the average distance travelled for each customer?

WITH customer_distance_cte AS (
  SELECT
    DISTINCT c.order_id,
    c.customer_id,
    r.distance :: NUMERIC
  FROM
    pizza_runner.customer_orders c
    INNER JOIN pizza_runner.runner_orders r ON c.order_id = r.order_id
  WHERE
    cancellation = 'n/a'
  ORDER BY
    customer_id
)
SELECT
  customer_id,
  ROUND(AVG(distance), 1)
FROM
  customer_distance_cte
GROUP BY
  customer_id
ORDER BY
  customer_id

-- 5.  What was the difference between the longest and shortest delivery times for all orders?

SELECT
  MAX(duration :: NUMERIC) - MIN(duration :: NUMERIC) AS difference
FROM
  pizza_runner.runner_orders
WHERE
  cancellation = 'n/a'

-- 6.  What was the average speed for each runner for each delivery and do you notice any trend for these values?

SELECT
  order_id,
  runner_id,
  EXTRACT(
    'hour'
    FROM
      pickup_time :: TIMESTAMP
  ) AS hour,
  ROUND(
    distance :: NUMERIC / (duration :: NUMERIC / 60),
    2
  ) AS avg_speed
FROM
  pizza_runner.runner_orders
WHERE
  cancellation = 'n/a'
ORDER BY runner_id

-- 7.  What is the successful delivery percentage for each runner?

SELECT
  DISTINCT runner_id,
  100 * COUNT(*) FILTER (
    WHERE
      cancellation = 'n/a'
  ) OVER (PARTITION BY runner_id) / COUNT(*) OVER (PARTITION BY runner_id) AS successful_delivery_percentage
FROM
  pizza_runner.runner_orders

-- INGREDIENT
-- 1.  What are the standard ingredients for each pizza?

WITH ingredients_list_cte AS (
  SELECT
    pizza_id,
    UNNEST(STRING_TO_ARRAY(toppings, ',')) :: INTEGER AS topping_id
  FROM
    pizza_runner.pizza_recipes
)
SELECT
  pizza_id,
  ARRAY_TO_STRING(ARRAY_AGG(topping_name), ', ') AS standard_ingredients
FROM
  ingredients_list_cte i
  INNER JOIN pizza_runner.pizza_toppings t ON i.topping_id = t.topping_id
GROUP BY
  pizza_id
ORDER BY 
  pizza_id

-- 2.  What was the most commonly added extra?

WITH extras_list_cte AS (
  SELECT
    UNNEST(STRING_TO_ARRAY(extras, ',')) :: INTEGER AS topping_id
  FROM
    pizza_runner.customer_orders
  WHERE
    extras <> 'none'
)
SELECT
  topping_name,
  COUNT(*) AS extras_count
FROM
  extras_list_cte e
  INNER JOIN pizza_runner.pizza_toppings t ON e.topping_id = t.topping_id
GROUP BY
  topping_name
ORDER BY extras_count DESC

-- 3.  What was the most common exclusion?

WITH exclusions_list_cte AS (
  SELECT
    UNNEST(STRING_TO_ARRAY(exclusions, ',')) :: INTEGER AS topping_id
  FROM
    pizza_runner.customer_orders
  WHERE
    exclusions <> 'none'
)
SELECT
  topping_name,
  COUNT(*) AS exclusions_count
FROM
  exclusions_list_cte e
  INNER JOIN pizza_runner.pizza_toppings t ON e.topping_id = t.topping_id
GROUP BY
  topping_name
ORDER BY
  exclusions_count DESC

-- 4.  Generate an order item for each record in the  `customers_orders`  table in the format of one of the following:
-- -   `Meat Lovers`
-- -   `Meat Lovers - Exclude Beef`
-- -   `Meat Lovers - Extra Bacon`
-- -   `Meat Lovers - Exclude Cheese, Bacon - Extra Mushroom, Peppers`

WITH toppings_list_cte AS (
  SELECT
    order_id,
    pizza_id,
    ROW_NUMBER() OVER (PARTITION BY order_id) AS order_pizza_id,
    UNNEST(STRING_TO_ARRAY(exclusions, ',')) AS exclusions_topping_id,
    UNNEST(STRING_TO_ARRAY(extras, ',')) AS extras_topping_id
  FROM
    pizza_runner.customer_orders
),
cleaned_topping_id AS (
  SELECT
    order_id,
    pizza_id,
    order_pizza_id,
    CASE
      WHEN exclusions_topping_id = 'none' THEN 0
      ELSE exclusions_topping_id :: INTEGER
    END AS exclusions_id,
    CASE
      WHEN extras_topping_id = 'none' THEN 0
      ELSE extras_topping_id :: INTEGER
    END AS extras_id
  FROM
    toppings_list_cte
),
extras_cte AS (
  SELECT
    order_id,
    pizza_name,
    order_pizza_id,
    ARRAY_TO_STRING(ARRAY_AGG(t.topping_name), ', ') AS extras_ingredients
  FROM
    cleaned_topping_id c
    LEFT JOIN pizza_runner.pizza_toppings t ON c.extras_id = t.topping_id
    INNER JOIN pizza_runner.pizza_names n ON c.pizza_id = n.pizza_id
  GROUP BY
    order_id,
    pizza_name,
    order_pizza_id
),
exclusions_cte AS (
  SELECT
    order_id,
    pizza_name,
    order_pizza_id,
    ARRAY_TO_STRING(ARRAY_AGG(t.topping_name), ', ') AS exclusions_ingredients
  FROM
    cleaned_topping_id c
    LEFT JOIN pizza_runner.pizza_toppings t ON c.exclusions_id = t.topping_id
    INNER JOIN pizza_runner.pizza_names n ON c.pizza_id = n.pizza_id
  GROUP BY
    order_id,
    pizza_name,
    order_pizza_id
),
final_output_cte AS (
  SELECT
    e.pizza_name,
    CASE
      WHEN exclusions_ingredients <> '' THEN '- Exclude ' || exclusions_ingredients
      ELSE ''
    END AS exclusions_text,
    CASE
      WHEN extras_ingredients <> '' THEN '- Extra ' || extras_ingredients
      ELSE ''
    END AS extras_text
  FROM
    extras_cte e
    INNER JOIN exclusions_cte e2 ON e.order_id = e2.order_id
    AND e.pizza_name = e2.pizza_name
    AND e.order_pizza_id = e2.order_pizza_id	
)
SELECT
  pizza_name || ' ' || exclusions_text || ' ' || extras_text AS pizza_order
FROM
  final_output_cte

-- 5.  Generate an alphabetically ordered comma separated ingredient list for each pizza order from the  `customer_orders`  table and add a  `2x`  in front of any relevant ingredients
-- -   For example:  `"Meat Lovers: 2xBacon, Beef, ... , Salami"`

-- 6.  What is the total quantity of each ingredient used in all delivered pizzas sorted by most frequent first?

WITH ingredients_cte AS (
  SELECT
    c.exclusions,
    c.extras,
    r.toppings
  FROM
    pizza_runner.customer_orders c
    INNER JOIN pizza_runner.pizza_recipes r ON c.pizza_id = r.pizza_id
),
unnested_ingredients_cte AS (
  SELECT
    UNNEST(STRING_TO_ARRAY(extras, ',')) AS topping_id
  FROM
    ingredients_cte
  UNION ALL
    (
      SELECT
        UNNEST(STRING_TO_ARRAY(toppings, ',')) AS topping_id
      FROM
        ingredients_cte
      EXCEPT ALL
      SELECT
        UNNEST(STRING_TO_ARRAY(exclusions, ',')) AS topping_id
      FROM
        ingredients_cte
    )
),
cleaned_topping_id AS (
  SELECT
    CASE
      WHEN topping_id = 'none' THEN 0
      ELSE topping_id :: INTEGER
    END AS topping_id
  FROM
    unnested_ingredients_cte
)
SELECT
  topping_name,
  COUNT(*) AS topping_count
FROM
  cleaned_topping_id c
  INNER JOIN pizza_runner.pizza_toppings t ON c.topping_id = t.topping_id
GROUP BY
  topping_name
ORDER BY
  topping_count DESC

-- PRICING AND RATINGS
-- 1.  If a Meat Lovers pizza costs $12 and Vegetarian costs $10 and there were no charges for changes - how much money has Pizza Runner made so far if there are no delivery fees?

SELECT
  SUM(CASE WHEN pizza_id = 1 THEN 12 ELSE 10 END)
FROM
  pizza_runner.customer_orders

-- 2.  What if there was an additional $1 charge for any pizza extras?

SELECT
  SUM(
    CASE
      WHEN pizza_id = 1 THEN 12
      ELSE 10
    END
  ) + SUM(
    CASE
      WHEN extras <> 'none' THEN CARDINALITY(STRING_TO_ARRAY(extras, ','))
      ELSE 0
    END
  ) AS pizza_cost
FROM
  pizza_runner.customer_orders c
WHERE
  EXISTS (
    SELECT
      1
    FROM
      pizza_runner.runner_orders r
    WHERE
      c.order_id = r.order_id
      AND cancellation = 'n/a'
  )

-- -   Add cheese is $1 extra

SELECT
  SUM(
    CASE
      WHEN pizza_id = 1 THEN 12
      ELSE 10
    END
  ) + SUM(
    CASE
      WHEN extras <> 'none' THEN CARDINALITY(STRING_TO_ARRAY(extras, ','))
      ELSE 0
    END
  ) + SUM(
    CASE
      WHEN '4' = ANY(STRING_TO_ARRAY(extras, ',')) THEN 1
      ELSE 0
    END
  ) AS pizza_cost
FROM
  pizza_runner.customer_orders c
WHERE
  EXISTS (
    SELECT
      1
    FROM
      pizza_runner.runner_orders r
    WHERE
      c.order_id = r.order_id
      AND cancellation = 'n/a'
  )

-- 3.  The Pizza Runner team now wants to add an additional ratings system that allows customers to rate their runner, 
-- how would you design an additional table for this new dataset - generate a schema for this new table 
-- and insert your own data for ratings for each successful customer order between 1 to 5.

SELECT
  SETSEED(1);
DROP TABLE IF EXISTS pizza_runner.ratings;
CREATE TABLE pizza_runner.ratings ("order_id" INTEGER, "rating" INTEGER);
INSERT INTO
  pizza_runner.ratings
SELECT
  order_id,
  FLOOR(1 + 5 * RANDOM()) AS rating
FROM
  pizza_runner.runner_orders
WHERE
  cancellation = 'n/a';

-- 4.  Using your newly generated table - can you join all of the information together 
-- to form a table which has the following information for successful deliveries?

SELECT
  DISTINCT c.order_id,
  c.customer_id,
  r.runner_id,
  ra.rating,
  c.order_time,
  r.pickup_time,
  DATE_PART('min', AGE(PICKUP_TIME :: TIMESTAMP, order_time)) :: INTEGER AS pickup_minutes,
  r.duration,
  ROUND(
    r.distance :: NUMERIC / (r.duration :: NUMERIC / 60),
    1
  ) AS avg_speed,
  COUNT(*) OVER (PARTITION BY c.order_id) AS pizza_count 
FROM
  pizza_runner.customer_orders c
  INNER JOIN pizza_runner.runner_orders r ON c.order_id = r.order_id
  INNER JOIN pizza_runner.ratings ra ON c.order_id = ra.order_id
WHERE
  r.cancellation = 'n/a'
ORDER BY
  order_id

-- 5.  If a Meat Lovers pizza was $12 and Vegetarian $10 fixed prices with no cost for extras and each runner is 
-- paid $0.30 per kilometre traveled - how much money does Pizza Runner have left over after these deliveries?

WITH sales_and_delivery_cte AS (
  SELECT
    c.order_id,
    0.3 * r.distance :: NUMERIC AS delivery_fee,
    SUM(
      CASE
        WHEN pizza_id = 1 THEN 12
        ELSE 10
      END
    ) AS order_price
  FROM
    pizza_runner.customer_orders c
    INNER JOIN pizza_runner.runner_orders r ON c.order_id = r.order_id
  WHERE
    EXISTS (
      SELECT
        1
      FROM
        pizza_runner.runner_orders r
      WHERE
        c.order_id = r.order_id
        AND cancellation = 'n/a'
    )
  GROUP BY
    c.order_id,
    r.distance
)
SELECT
  SUM(order_price) - SUM(delivery_fee) AS sales_net_delivery
FROM
  sales_and_delivery_cte