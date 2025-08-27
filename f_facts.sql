/***
AI Generated SQL
***/
WITH lineitem AS (
  SELECT
    l_orderkey,
    l_linenumber,
    l_partkey,
    l_suppkey,
    l_extendedprice,
    l_tax,
    l_discount,
    l_quantity,
    l_returnflag,
    l_linestatus,
    l_shipinstruct,
    l_shipmode,
    l_shipdate,
    l_commitdate,
    l_receiptdate,
    l_comment
  FROM delta.`dbfs:/databricks-datasets/tpch/delta-001/lineitem/` AS lineitem
),
orders AS (
  SELECT
    o_custkey,
    o_orderdate,
    o_orderstatus,
    o_totalprice,
    o_orderpriority,
    o_clerk,
    o_shippriority,
    o_comment,
    o_orderkey
  FROM delta.`dbfs:/databricks-datasets/tpch/delta-001/orders/` AS orders
),
part AS (
  SELECT
    p_partkey,
    p_name,
    p_mfgr,
    p_brand,
    p_type,
    p_size,
    p_container,
    p_retailprice,
    p_comment
  FROM delta.`dbfs:/databricks-datasets/tpch/delta-001/part/` AS part
),
partsupp AS (
  SELECT
    ps_availqty,
    ps_supplycost,
    ps_comment,
    ps_partkey,
    ps_suppkey
  FROM delta.`dbfs:/databricks-datasets/tpch/delta-001/partsupp/` AS partsupp
),
supplier AS (
  SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    s_acctbal,
    s_comment
  FROM delta.`dbfs:/databricks-datasets/tpch/delta-001/supplier/` AS supplier
),
customer AS (
  SELECT
    c_custkey,
    c_name,
    c_acctbal,
    c_nationkey,
    c_mktsegment,
    c_address,
    c_phone,
    c_comment
  FROM delta.`dbfs:/databricks-datasets/tpch/delta-001/customer/` AS customer
),
nation AS (
  SELECT
    n_name,
    n_comment,
    n_nationkey,
    n_regionkey
  FROM delta.`dbfs:/databricks-datasets/tpch/delta-001/nation/` AS nation
),
region AS (
  SELECT
    r_name,
    r_comment,
    r_regionkey
  FROM delta.`dbfs:/databricks-datasets/tpch/delta-001/region/` AS region
),
base_fact AS (
  SELECT
    lineitem.*,
    orders.*,
    part.*,
    partsupp.*,
    supplier.*
  FROM lineitem
  JOIN orders ON lineitem.l_orderkey = orders.o_orderkey
  JOIN part ON lineitem.l_partkey = part.p_partkey
  JOIN partsupp ON part.p_partkey = partsupp.ps_partkey AND lineitem.l_suppkey = partsupp.ps_suppkey
  JOIN supplier ON partsupp.ps_suppkey = supplier.s_suppkey
),
cust_geo AS (
  SELECT
    customer.*,
    nation.*,
    region.*
  FROM customer
  JOIN nation ON customer.c_nationkey = nation.n_nationkey
  JOIN region ON nation.n_regionkey = region.r_regionkey
),
enriched_cte AS (
  SELECT
    base_fact.*,
    cust_geo.*
  FROM base_fact
  JOIN cust_geo ON base_fact.o_custkey = cust_geo.c_custkey
),
daily_agg AS (
  SELECT
    o_orderdate,
    r_name,
    p_brand,
    l_shipmode,
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) AS daily_revenue,
    COUNT(DISTINCT o_orderkey) AS daily_orders,
    COUNT(DISTINCT p_partkey) AS distinct_parts_per_day
  FROM enriched_cte
  GROUP BY o_orderdate, r_name, p_brand, l_shipmode, n_name
),
monthly_agg AS (
  SELECT
    DATE_TRUNC('month', o_orderdate) AS month,
    r_name,
    p_brand,
    l_shipmode,
    n_name,
    SUM(daily_revenue) AS monthly_revenue,
    AVG(daily_revenue) AS avg_daily_revenue,
    MAX(daily_revenue) AS max_daily_revenue,
    COUNT(DISTINCT distinct_parts_per_day) AS distinct_parts_sold,
    RANK() OVER (PARTITION BY r_name, DATE_TRUNC('month', o_orderdate) ORDER BY SUM(daily_revenue) DESC) AS region_month_rank
  FROM daily_agg
  GROUP BY DATE_TRUNC('month', o_orderdate), r_name, p_brand, l_shipmode, n_name
)

SELECT
  monthly_agg.month,
  monthly_agg.r_name AS region_name,
  monthly_agg.n_name AS nation,
  enriched.p_brand AS brand,
  enriched.p_mfgr AS manufacturer,
  monthly_agg.l_shipmode AS shipmode,
  monthly_agg.monthly_revenue,
  monthly_agg.avg_daily_revenue,
  monthly_agg.max_daily_revenue,
  monthly_agg.distinct_parts_sold,
  MAX(enriched.o_comment) AS order_comment,
  MAX(enriched.p_comment) AS part_comment,
  MAX(enriched.l_comment) AS lineitem_comment,
  MAX(enriched.l_shipdate) AS latest_ship_date,
  MAX(enriched.l_commitdate) AS latest_commit_date,
  MAX(enriched.l_receiptdate) AS latest_receipt_date,
  MAX(enriched.ps_supplycost) AS avg_supply_cost,
  MAX(enriched.ps_comment) AS partsupplier_comment,
  MAX(enriched.c_mktsegment) AS customer_segment,
  SUM(enriched.c_acctbal) AS customer_acctbal,
  MAX(enriched.c_comment) AS customer_comment,
  MAX(enriched.n_comment) AS nation_comment,
  MAX(enriched.r_comment) AS region_comment,
  CURRENT_TIMESTAMP AS entry_date,
  CURRENT_TIMESTAMP AS update_date
FROM monthly_agg
JOIN enriched_cte AS enriched ON monthly_agg.r_name = enriched.r_name AND DATE_TRUNC('month', enriched.o_orderdate) = monthly_agg.month
WHERE monthly_agg.region_month_rank <= 3
GROUP BY 
  monthly_agg.month,
  monthly_agg.r_name,
  monthly_agg.n_name,
  enriched.p_brand,
  enriched.p_mfgr,
  monthly_agg.l_shipmode,
  monthly_agg.monthly_revenue,
  monthly_agg.avg_daily_revenue,
  monthly_agg.max_daily_revenue,
  monthly_agg.distinct_parts_sold
  LIMIT 10