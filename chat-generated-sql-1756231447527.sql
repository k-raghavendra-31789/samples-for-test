/***
AI Generated SQL
***/
SELECT
  lineitem.l_orderkey AS order_key,
  orders.o_orderdate AS order_date,
  orders.o_custkey AS cust_key,
  lineitem.l_partkey AS part_key,
  lineitem.l_suppkey AS supp_key,
  lineitem.l_quantity AS quantity,
  lineitem.l_extendedprice AS extended_price,
  lineitem.l_discount AS discount,
  (lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS total_price
FROM delta.`dbfs:/sample/tpch/lineitem/` AS lineitem
LEFT OUTER JOIN delta.`dbfs:/sample/tpch/orders/` AS orders
  ON lineitem.l_orderkey = orders.o_orderkey;