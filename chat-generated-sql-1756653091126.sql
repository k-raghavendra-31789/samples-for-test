/***
AI Generated SQL
***/
WITH lineitem AS (
SELECT
l_orderkey,
l_linenumber,
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
l_comment,
l_partkey,
l_suppkey
FROM delta.`dbfs:/databricks-datasets/tpch/delta-001/lineitem/` AS lineitem
),
orders AS (
SELECT
o_orderkey,
o_custkey,
o_orderdate,
o_orderstatus,
o_totalprice,
o_orderpriority,
o_clerk,
o_shippriority,
o_comment
FROM delta.`dbfs:/databricks-datasets/tpch/delta-001/orders/` AS orders
),
part AS (
SELECT
p_partkey,
p_name AS part_name,
p_mfgr AS part_mfgr,
p_brand AS part_brand,
p_type AS part_type,
p_size AS part_size,
p_container AS part_container,
p_retailprice,
p_comment
FROM delta.`dbfs:/databricks-datasets/tpch/delta-001/part/` AS part
),
partsupp AS (
SELECT
ps_partkey,
ps_suppkey,
ps_availqty,
ps_supplycost,
ps_comment
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
c_mktsegment,
c_address,
c_phone,
c_comment,
c_nationkey
FROM delta.`dbfs:/databricks-datasets/tpch/delta-001/customer/` AS customer
),
nation AS (
SELECT
n_nationkey,
n_name,
n_regionkey,
n_comment
FROM delta.`dbfs:/databricks-datasets/tpch/delta-001/nation/` AS nation
),
region AS (
SELECT
r_regionkey,
r_name,
r_comment
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
o_orderdate AS orderdate,
r_name AS region_name,
part_brand,
l_shipmode AS shipmode,
SUM(l_extendedprice * (1 - l_discount)) AS daily_revenue,
COUNT(DISTINCT l_orderkey) AS daily_orders,
COUNT(DISTINCT p_partkey) AS distinct_parts_per_day
FROM enriched_cte
GROUP BY o_orderdate, r_name, part_brand, l_shipmode
),
monthly_agg AS (
SELECT
DATE_TRUNC('month', orderdate) AS month,
region_name,
n_name AS nation_name,
part_mfgr,
e.part_brand,
shipmode,
SUM(daily_revenue) AS monthly_revenue,
AVG(daily_revenue) AS average_daily_revenue,
MAX(daily_revenue) AS max_daily_revenue,
COUNT(DISTINCT distinct_parts_per_day) AS distinct_parts_sold,
RANK() OVER (PARTITION BY region_name, DATE_TRUNC('month', orderdate) ORDER BY SUM(daily_revenue) DESC) AS region_month_rank
FROM daily_agg
JOIN enriched_cte e ON daily_agg.orderdate = e.o_orderdate AND daily_agg.region_name = e.r_name
GROUP BY DATE_TRUNC('month', orderdate), region_name, n_name, part_mfgr, e.part_brand, shipmode
)

SELECT
m.month,
m.region_name,
m.nation_name,
m.part_brand,
m.part_mfgr,
m.shipmode,
m.monthly_revenue,
m.average_daily_revenue,
m.max_daily_revenue,
m.distinct_parts_sold,
MAX(e.o_comment) AS order_comment,
MAX(e.p_comment) AS part_comment,
MAX(e.l_comment) AS lineitem_comment,
MAX(e.l_shipdate) AS latest_ship_date,
MAX(e.l_commitdate) AS latest_commit_date,
MAX(e.l_receiptdate) AS latest_receipt_date,
MAX(e.ps_supplycost) AS avg_supply_cost,
MAX(e.ps_comment) AS partsupp_comment,
MAX(e.c_mktsegment) AS customer_segment,
MAX(e.c_acctbal) AS customer_acctbal,
MAX(e.c_comment) AS customer_comment,
MAX(e.n_comment) AS nation_comment,
MAX(e.r_comment) AS region_comment
FROM monthly_agg m
JOIN enriched_cte e ON m.region_name = e.r_name AND DATE_TRUNC('month', e.o_orderdate) = m.month
WHERE m.region_month_rank <= 3
GROUP BY
m.month,
m.region_name,
m.nation_name,
m.part_brand,
m.part_mfgr,
m.shipmode,
m.monthly_revenue,
m.average_daily_revenue,
m.max_daily_revenue,
m.distinct_parts_sold
LIMIT 10