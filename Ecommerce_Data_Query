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
