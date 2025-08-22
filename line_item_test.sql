/***
Samples for AI DE Pair programmng APP
Env: Databricks
Unity: Yes
***/
With line_item_sales AS (
    SELECT 
        l_orderkey as orderkey,l_partkey as partkey,l_suppkey as suppkey,l_quantity as quantity,l_extendedprice as extendedprice,l_discount  as discount
    FROM samples.tpch.lineitem
    )

    SELECT 
        line_item_sales.orderkey    AS order_key,
        orders.o_orderdate            AS order_date,
        orders.o_custkey              AS cust_key,
        line_item_sales.partkey     AS part_key,
        line_item_sales.suppkey     AS supp_key,
        line_item_sales.quantity    AS quantity,
        line_item_sales.extendedprice AS extended_price,
        line_item_sales.discount     AS discount,
        (line_item_sales.extendedprice * (1 - line_item_sales.discount)) AS revenue

    FROM  line_item_sales
LEFT OUTER JOIN samples.tpch.orders AS orders ON line_item_sales.orderkey = orders.o_orderkey
