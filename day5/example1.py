from pyspark.sql import SparkSession, functions as F
from schema.tpch_schema import order_schema,   customer_schema ,   lineitem_schema, region_schema, nation_schema, part_schema, supplier_schema

spark = (SparkSession.builder.
         appName("week1").
         master("local[*]").
         config("spark.driver.memory","2g").
         getOrCreate())

print(spark.version)


"""
**Tasks**
- [ ] Join `lineitem → orders → customer → nation → region` to get every line with its customer's nation and region.
- [ ] Add `part` and `supplier` joins to enrich lines with product and supplier info.
- [ ] Revenue by region and by nation (now that you can).
- [ ] Use `F.broadcast` on `nation`/`region` and confirm in `explain()` that it's a broadcast join.
- [ ] Try a `left_anti` join to find orders with no matching line items (should be none — verify).
"""

lineitem = (spark.read.option("sep","|").
            option("header",False).
            schema(lineitem_schema).
            csv("data/tpch/lineitem.tbl"))
orders = (spark.read.option("sep","|").
            option("header",False).
            schema(order_schema).
            csv("data/tpch/orders.tbl"))
customer = (spark.read.option("sep","|").
            option("header",False).
            schema(customer_schema).
            csv("data/tpch/customer.tbl"))

nation = (spark.read.option("sep","|").
            option("header",False).
            schema(nation_schema).
            csv("data/tpch/nation.tbl"))


region = (spark.read.option("sep","|").
            option("header",False).
            schema(region_schema).
            csv("data/tpch/region.tbl"))



line_orders = lineitem.join(
    orders,
    lineitem["l_orderkey"] == orders["o_orderkey"],
    "inner"
)

line_orders_customer = line_orders.join(
    customer,
    line_orders["o_custkey"] == customer["c_custkey"],
    "inner"
)

# line_orders_customer_nation = line_orders_customer.join(
#     nation,
#     line_orders_customer["c_nationkey"] == nation["n_nationkey"],
#     "inner"
# )


line_orders_customer_nation = line_orders_customer.join(
    F.broadcast(nation),
    line_orders_customer["c_nationkey"] == nation["n_nationkey"],
    "inner"
)

# enriched = line_orders_customer_nation.join(
#     region,
#     line_orders_customer_nation["n_regionkey"] == region["r_regionkey"],
#     "inner"
# )

enriched = line_orders_customer_nation.join(
    F.broadcast(region),
    line_orders_customer_nation["n_regionkey"] == region["r_regionkey"],
    "inner"
)

#enriched.explain()

#enriched.show()
# so i kept () here its for the multi line formating both would work
part = (spark.read.option("sep", "|")
        .option("header", False)
        .schema(part_schema)
        .csv("data/tpch/part.tbl")
)

supplier = spark.read.option("sep", "|").option("header", False).schema(supplier_schema).csv("data/tpch/supplier.tbl")


enriched_with_part = enriched.join(
    part,
    enriched["l_partkey"] == part["p_partkey"],
    "inner"
)

final_enriched = enriched_with_part.join(
    supplier,
    enriched_with_part["l_suppkey"] == supplier["s_suppkey"],
    "inner"
)

#final_enriched.show(10)


final_enriched = final_enriched.withColumn(
    "revenue",
    F.col("l_extendedprice") * (1 - F.col("l_discount"))
)

#final_enriched.groupBy("r_name","n_name").agg(F.sum("revenue").alias("total_revenue")).show()

orders.join(
    lineitem,
    orders["o_orderkey"]==lineitem["l_orderkey"],
    "left_anti"
).show()