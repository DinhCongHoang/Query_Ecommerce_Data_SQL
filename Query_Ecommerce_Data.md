# Google Data Analytic Session Query
<p align="right">Using Google Bigquery</p>

## Questions

### Query 01: calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)
- First, create a CTE to get the total visits, pageviews, transactions, fullVisitorId and visitId of each session in Jan, Feb and March 2017.
- Use the above CTE to get total visits, pageviews, transactions of each month.

```sql
WITH get_data AS (
  SELECT
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
    fullVisitorId,
    visitId,
    totals.visits AS visits,
    totals.pageviews AS pageviews,
    totals.transactions AS transactions
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  WHERE _table_suffix BETWEEN '0101' AND '0331'
)

SELECT
  month,
  SUM(visits) AS visits,
  SUM(pageviews) AS pageviews,
  SUM(transactions) AS transactions
FROM get_data
GROUP BY month
ORDER BY month;
```
#### Result
![image](https://github.com/DinhCongHoang/Query_Ecommerce_Data_SQL/assets/45199893/41914bd4-a871-494e-bce1-78f29c1225d1)

#
### Query 02: Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC)
- Create a CTE to get visitId, number of visits, number of bounces and traffic source from each session in July 2017.
- From above CTE, calculate total visits, total bounces, bounce rate of each source.

```sql
WITH get_data AS (
  SELECT
    visitId,
    totals.visits AS visits,
    totals.bounces AS bounces,
    trafficSource.source AS source
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
  WHERE (_table_suffix BETWEEN '01' AND '31')
)

SELECT
  source,
  SUM(visits) AS total_visits,
  SUM(bounces) AS total_no_of_bounces,
  SUM(bounces) / SUM(visits) * 100.0 AS bounce_rate
FROM get_data
GROUP BY source
ORDER BY total_visits DESC
```
#### Result
- The image below represents first 6 records with highest total visits
![image](https://github.com/DinhCongHoang/Query_Ecommerce_Data_SQL/assets/45199893/7d43085f-749a-46ec-b0c7-5f593b96801c)

#
### Query 3: Revenue by traffic source by week, by month in June 2017
- Create 'monthly' CTE to get revenue by sources in June 2017
- Create 'weekly' CTE to get revenue by weeks in June 2017
- Combine results of above CTEs to get the final result
- Revenue has to be divided by 1,000,000 because the value passed to the dataset is multiplied by 10^6 (e.g. 2.40 would be given as 2,400,000)
```sql
WITH monthly AS (
  SELECT
    'Month' AS time_type,
    FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date)) AS time,
    trafficSource.source AS source,
    SUM(product.productRevenue) / 1000000.0 AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
  WHERE (_table_suffix BETWEEN '01' AND '30')
  GROUP BY time, source
),

weekly AS (
  SELECT
    'Week' AS time_type,
    FORMAT_DATE("%Y%U", PARSE_DATE("%Y%m%d", date)) AS time,
    trafficSource.source AS source,
    SUM(product.productRevenue) / 1000000.0 AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
  WHERE (_table_suffix BETWEEN '01' AND '30')
  GROUP BY time, source
)

SELECT *
FROM (SELECT * FROM monthly
UNION ALL
SELECT * FROM weekly)
ORDER BY revenue DESC, source, time_type, time;
```
#### Result
- Below image shows top 6 rows with highest revenue.
![image](https://github.com/DinhCongHoang/Query_Ecommerce_Data_SQL/assets/45199893/37bac6d0-c2e2-4d2d-acf0-a9aa70c9fd01)

#
### Query 04: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017
- Create 'visitor' CTE to get the unique visitor ID, their type (purchaser or non-purchaser) in each month.
  - COUNT(product.productRevenue) > 0 means that revenue occurred in a month => Visitor purchased in that month
  - COUNT(product.productRevenue) = 0 means that revenue didn't occur in a month => Visitor didn't purchased in that month
- Create 'pgviews_by_visit' CTE to get month, fullVisitorId, visitId, number of pageviews of each visit
- Create 'avg_pgviews_purchase_cte' and 'avg_pgviews_nonpurchase_cte' CTE to calculate the average number of pageviews by each type of visitor
- Finally join 2 CTEs above to get final result.

```sql
WITH visitor AS (
  SELECT 
    FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date)) AS month,
    fullVisitorId,
    CASE WHEN COUNT(product.productRevenue) > 0 THEN 'purchaser' ELSE 'non-purchaser' END AS visitor_type
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
  WHERE (_table_suffix BETWEEN '0601' AND '0731')
  GROUP BY month, fullVisitorId
),

pgviews_by_visit AS (
  SELECT
    FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date)) AS month,
    fullVisitorId,
    visitId,
    totals.pageviews AS pgviews
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  WHERE (_table_suffix BETWEEN '0601' AND '0731')
),

avg_pgviews_purchase_cte AS (
  SELECT p.month,
    SUM(pgviews) / COUNT(DISTINCT p.fullVisitorId) AS avg_pgviews_purchase
  FROM pgviews_by_visit p
  JOIN visitor v
  ON p.month = v.month AND p.fullVisitorId = v.fullVisitorId
  WHERE visitor_type = 'purchaser'
  GROUP BY month
),

avg_pgviews_nonpurchase_cte AS (
  SELECT p.month,
    SUM(pgviews) / COUNT(DISTINCT p.fullVisitorId) AS avg_pgviews_non_purchase
  FROM pgviews_by_visit p
  JOIN visitor v
  ON p.month = v.month AND p.fullVisitorId = v.fullVisitorId
  WHERE visitor_type = 'non-purchaser'
  GROUP BY month
)

SELECT a1.month,
  avg_pgviews_purchase,
  avg_pgviews_non_purchase
FROM avg_pgviews_purchase_cte a1
JOIN avg_pgviews_nonpurchase_cte a2
ON a1.month = a2.month
ORDER BY a1.month
```
#### Result
![image](https://github.com/DinhCongHoang/Query_Ecommerce_Data_SQL/assets/45199893/f4882f73-76d4-4dd0-ae6a-55c70860e3bb)

#
### Query 05: Average number of transactions per user that made a purchase in July 2017
- Create 'purchVisitor' CTE to get unique fullVisitorId of visitors who purchased
- Create 'transactions_by_visit' CTE to get total transactions of each visit
- Join 2 CTEs to get the transactions of all visitors who purchased, then calculate average number of transactions per user who purchased.

```sql
WITH purchVisitor AS (
  SELECT 
    FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date)) AS month,
    fullVisitorId
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
  WHERE (_table_suffix BETWEEN '01' AND '31')
  GROUP BY month, fullVisitorId
  HAVING COUNT(product.productRevenue) > 0
),

transactions_by_visit AS (
  SELECT
    DISTINCT FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date)) AS month,
    fullVisitorId,
    visitId,
    totals.transactions AS transactions
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` t
  WHERE (_table_suffix BETWEEN '01' AND '31')
)

SELECT t.month,
  SUM(transactions) / COUNT(DISTINCT t.fullVisitorId) AS avg_total_transactions_per_user
FROM transactions_by_visit t
JOIN purchVisitor p
ON t.month = p.month AND t.fullVisitorId = p.fullVisitorId
GROUP BY t.month
```
#### Result
![image](https://github.com/DinhCongHoang/Query_Ecommerce_Data_SQL/assets/45199893/2ce9bcee-ca80-4a57-99f5-56a13207e178)

#
### Query 06: Average amount of money spent per session. Only include purchaser data in July 2017
- Create `get_data` CTE to get total product revenue of each visit
- Calculate average revenue by visits 

```sql
WITH get_data AS (
  SELECT
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
    fullVisitorId,
    visitId,
    SUM(productRevenue) AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
  WHERE (_table_suffix BETWEEN '01' AND '31')
    AND totals.transactions > 0
    AND product.productRevenue IS NOT NULL
  GROUP BY month, fullVisitorId, visitId
)

--Tính doanh thu trung bình của từng session
SELECT
  month,
  ROUND(SUM(revenue) / COUNT(visitId) / 1000000.0, 2) AS avg_revenue_by_user_per_visit
FROM get_data
GROUP BY month;
```
#### Result
![image](https://github.com/DinhCongHoang/Query_Ecommerce_Data_SQL/assets/45199893/362f892e-0e7d-4672-a2d1-421ab346e2cd)

#
### Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
- Create `get_data` CTE to get product name and quantity of visits in which "YouTube Men's Vintage Henley" was purchased.
- Get other product purchased within the visits that have "YouTube Men's Vintage Henley"

```sql
WITH get_data AS (
  SELECT
    fullVisitorId,
    visitId,
    product.v2ProductName,
    product.productQuantity
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    UNNEST(hits) hits,
    UNNEST(hits.product) product
    WHERE (_table_suffix BETWEEN '01' AND '31')
      AND totals.transactions > 0
      AND product.productRevenue IS NOT NULL 
      AND visitId IN (
        --Get all visitId in which "YouTube Men's Vintage Henley" was purchased
        SELECT
          visitId
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
        UNNEST(hits) hits,
        UNNEST(hits.product) product
        WHERE (_table_suffix BETWEEN '01' AND '31')
          AND totals.transactions > 0
          AND product.productRevenue IS NOT NULL
          AND v2ProductName = "YouTube Men's Vintage Henley"
        )
)

--Dựa vào CTE trên, tính tổng số lượng mua của từng tên hàng, bỏ "YouTube Men's Vintage Henley"
SELECT
  v2ProductName AS other_purchased_products,
  SUM(productQuantity) AS quantity
FROM get_data
WHERE v2ProductName <> "YouTube Men's Vintage Henley"
GROUP BY v2ProductName
ORDER BY quantity DESC
```
#### Result
- Below image shows top 6 other purchased products
![image](https://github.com/DinhCongHoang/Query_Ecommerce_Data_SQL/assets/45199893/a8db70db-6ff9-445d-80c7-77833319d4d7)

#
### Query 08: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. For example, 100% product view then 40% add_to_cart and 10% purchase.
#### Add_to_cart_rate = number product  add to cart/number product view. Purchase_rate = number product purchase/number product view. The output should be calculated in product level.
- Create `product_view` CTE to get number of product views, add-to-cart, purchases
- 

```sql
WITH product_view AS (
  SELECT
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
    SUM(CASE WHEN eCommerceAction.action_type = '2' THEN 1 ELSE 0 END) AS num_product_view,
    SUM(CASE WHEN eCommerceAction.action_type = '3' THEN 1 ELSE 0 END) AS num_addtocart,
    SUM(CASE WHEN eCommerceAction.action_type = '6' THEN 1 ELSE 0 END) AS num_purchase,
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
  WHERE (_table_suffix BETWEEN '0101' AND '0331')
  GROUP BY month
)

SELECT
  month,
  num_product_view,
  num_addtocart,
  num_purchase,
  ROUND(num_addtocart / num_product_view * 100.0, 2) AS add_to_cart_rate,
  ROUND(num_purchase / num_product_view * 100.0, 2) AS purchase_rate
FROM product_view
```
#### Result
![image](https://github.com/DinhCongHoang/Query_Ecommerce_Data_SQL/assets/45199893/be641389-0dd4-4c8d-9e2c-3a3f49b6b865)

