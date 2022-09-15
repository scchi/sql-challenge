-- 1. What is the total amount each customer spent at the restaurant?

WITH joined_sales_menu AS (
  SELECT
    sales.customer_id,
    sales.order_date,
    menu.price
  FROM
    dannys_diner.sales
    INNER JOIN dannys_diner.menu ON sales.product_id = menu.product_id
)

SELECT
  customer_id,
  sum(price)
FROM
  joined_sales_menu
GROUP BY
  customer_id
ORDER BY
  customer_id

-- 2. How many days has each customer visited the restaurant?

SELECT
  customer_id,
  COUNT(DISTINCT order_date)
FROM
  joined_sales_menu
GROUP BY
  customer_id

-- 3. What was the first item from the menu purchased by each customer?

WITH ranked_order_by_date AS (
  SELECT
    sales.customer_id,
    menu.product_name,
    RANK() OVER (
      PARTITION BY customer_id
      ORDER BY
        order_date
    ) AS order_rank
  FROM
    dannys_diner.sales
    INNER JOIN dannys_diner.menu ON sales.product_id = menu.product_id
)

SELECT
  DISTINCT customer_id,
  product_name
FROM
  ranked_order_by_date
WHERE
  order_rank = 1

-- 4. What is the most purchased item on the menu and how many times was it purchased by all customers?

SELECT
  menu.product_name,
  COUNT(*) AS order_count
FROM
  dannys_diner.sales
  INNER JOIN dannys_diner.menu ON sales.product_id = menu.product_id
GROUP BY
  menu.product_name
ORDER BY
  order_count DESC
LIMIT
  1

-- 5. Which item was the most popular for each customer?

WITH ranked_product_per_customer_by_count AS (
  SELECT
    sales.customer_id,
    menu.product_name,
    COUNT(*) as product_quantity,
    RANK() OVER (
      PARTITION BY sales.customer_id
      ORDER BY
        COUNT(*) DESC
    ) AS product_rank
  FROM
    dannys_diner.sales
    INNER JOIN dannys_diner.menu ON sales.product_id = menu.product_id
  GROUP BY
    sales.customer_id,
    menu.product_name
)

SELECT
  customer_id,
  product_name,
  product_quantity
FROM
  ranked_product_per_customer
WHERE
  product_rank = 1

-- 6. Which item was purchased first by the customer after they became a member (including the date they joined)?

WITH ranked_product_per_customer_by_date AS (
  SELECT
    sales.customer_id,
    menu.product_name,
    RANK() OVER (
      PARTITION BY sales.customer_id
      ORDER BY
        order_date
    ) AS product_rank
  FROM
    dannys_diner.members
    INNER JOIN dannys_diner.sales ON members.customer_id = sales.customer_id
    INNER JOIN dannys_diner.menu ON sales.product_id = menu.product_id
  WHERE
    sales.order_date >= members.join_date
)

SELECT
  customer_id,
  product_name
FROM
  ranked_product_per_customer_by_date
WHERE
  product_rank = 1

-- 7. Which item was purchased just before the customer became a member?

WITH ranked_order_per_customer_before_membership AS (
  SELECT
    sales.customer_id,
    sales.order_date,
    menu.product_name,
    RANK() OVER (
      PARTITION BY sales.customer_id
      ORDER BY
        order_date DESC
    ) AS product_rank
  FROM
    dannys_diner.members
    INNER JOIN dannys_diner.sales ON members.customer_id = sales.customer_id
    INNER JOIN dannys_diner.menu ON sales.product_id = menu.product_id
  WHERE
    sales.order_date < members.join_date
)

SELECT
  customer_id,
  order_date,
  product_name
FROM
  ranked_order_per_customer_before_membership
WHERE
  product_rank = 1

-- 8. What is the total items and amount spent for each member before they became a member?

SELECT
  sales.customer_id,
  COUNT(DISTINCT sales.product_id) AS unique_items_ordered,
  SUM(menu.price) AS total_spent
FROM
  dannys_diner.members
  INNER JOIN dannys_diner.sales ON members.customer_id = sales.customer_id
  INNER JOIN dannys_diner.menu ON sales.product_id = menu.product_id
WHERE
  sales.order_date < members.join_date
GROUP BY
  sales.customer_id

-- 9. If each $1 spent equates to 10 points and sushi has a 2x points multiplier, 
-- how many points would each customer have?

WITH points_per_order AS (
  SELECT
    sales.customer_id,
    menu.product_name,
    CASE
      WHEN menu.product_name = 'sushi' THEN menu.price * 20
      ELSE menu.price * 10
    END AS points
  FROM
    dannys_diner.sales
    INNER JOIN dannys_diner.menu ON sales.product_id = menu.product_id
)

SELECT
  customer_id,
  sum(points) AS total_points
FROM
  points_per_order
GROUP BY
  customer_id
ORDER BY total_points DESC

--  10. In the first week a customer joins the program (including their joining date), they earn 2x points on all items, not just sushi. 
-- How many points do customers A and B have at the end of January?

WITH joining_month_orders AS (
  SELECT
    sales.customer_id,
    sales.product_id,    
    sales.order_date,
    members.join_date,
    members.join_date + INTERVAL '6 days' AS promo_end
  FROM
    dannys_diner.members
    INNER JOIN dannys_diner.sales ON members.customer_id = sales.customer_id
  WHERE
    (
      EXTRACT(
        'year'
        FROM
          order_date
      ) = EXTRACT(
        'year'
        FROM
          join_date
      )
    )
    AND (
      EXTRACT(
        'month'
        FROM
          order_date
      ) = EXTRACT(
        'month'
        FROM
          join_date
      )
    )
)

SELECT
  customer_id,
  SUM(
    CASE
      WHEN (
        (
          order_date BETWEEN join_date
          AND promo_end
        )
        OR (menu.product_name = 'sushi')
      ) THEN 20 * menu.price
      ELSE 10 * menu.price
    END
  ) AS total_points_january
FROM
  joining_month_orders
  INNER JOIN dannys_diner.menu ON joining_month_orders.product_id = menu.product_id
GROUP BY
  customer_id

-- 11. Recreate the following table using the available data.

-- customer_id	  order_date	  product_name	  price	  member
--     A	        2021-01-01	      curry	       15	      N
--     A	        2021-01-01	      sushi	       10	      N
--     A	        2021-01-07	      curry	       15	      Y
--     A	        2021-01-10	      ramen	       12	      Y
--     A	        2021-01-11	      ramen	       12	      Y
--     A	        2021-01-11	      ramen	       12     	Y
--     B	        2021-01-01	      curry	       15	      N
--     B	        2021-01-02	      curry	       15	      N
--     B	        2021-01-04	      sushi	       10	      N
--     B	        2021-01-11	      sushi	       10	      Y
--     B	        2021-01-16	      ramen	       12	      Y
--     B	        2021-02-01	      ramen	       12	      Y
--     C	        2021-01-01	      ramen	       12	      N
--     C	        2021-01-01	      ramen	       12	      N
--     C	        2021-01-07	      ramen	       12	      N

--  12.  Danny also requires further information about the  `ranking`  of customer products, 
-- but he purposely does not need the ranking for non-member purchases so he expects null  
-- `ranking`  values for the records when customers are not yet part of the loyalty program.

WITH orders_with_membership_flag AS (
  SELECT
    sales.customer_id,
    sales.order_date,
    menu.product_name,
    menu.price,
    CASE
      WHEN members.customer_id IS NULL
      OR order_date < join_date THEN 'N'
      ELSE 'Y'
    END AS member
  FROM
    dannys_diner.sales
    LEFT JOIN dannys_diner.members ON sales.customer_id = members.customer_id
    INNER JOIN dannys_diner.menu ON sales.product_id = menu.product_id
  ORDER BY
    sales.customer_id,
    sales.order_date,
    menu.price DESC
)

SELECT
  customer_id,
  order_date,
  product_name,
  price,
  member,
  CASE
    WHEN member = 'N' THEN NULL
    ELSE RANK() OVER (
      PARTITION BY customer_id,
      member
      ORDER BY
        order_date
    )
  END AS ranking
FROM
  orders_with_membership_flag