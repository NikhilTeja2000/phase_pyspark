from pyspark.sql import SparkSession, functions as F
import pyspark.sql.types as T


spark= (SparkSession.builder.appName("task1").config("spark.builder.memory","2g").master("local[*]").getOrCreate())


print(spark.version)

#TODO: Create a class or similar..thing and import all the repeated part...next time.

order_schema = T.StructType([
    T.StructField("o_orderkey", T.LongType()),
    T.StructField("o_custkey", T.LongType()),
    T.StructField("o_orderstatus", T.StringType()),
    T.StructField("o_totalprice", T.DoubleType()),
    T.StructField("o_orderdate", T.DateType()),
    T.StructField("o_orderpriority", T.StringType()),
    T.StructField("o_clerk", T.StringType()),
    T.StructField("o_shippriority", T.IntegerType()),
    T.StructField("o_comment", T.StringType()),
])

order= (spark.read.option("sep","|").option("header",False).schema(order_schema).csv("data/tpch/orders.tbl"))



lineitem_schema = T.StructType([
    T.StructField("l_orderkey", T.LongType()),    T.StructField("l_partkey", T.LongType()),
    T.StructField("l_suppkey", T.LongType()),     T.StructField("l_linenumber", T.IntegerType()),
    T.StructField("l_quantity", T.DoubleType()),  T.StructField("l_extendedprice", T.DoubleType()),
    T.StructField("l_discount", T.DoubleType()),  T.StructField("l_tax", T.DoubleType()),
    T.StructField("l_returnflag", T.StringType()),T.StructField("l_linestatus", T.StringType()),
    T.StructField("l_shipdate", T.DateType()),     T.StructField("l_commitdate", T.DateType()),
    T.StructField("l_receiptdate", T.DateType()),  T.StructField("l_shipinstruct", T.StringType()),
    T.StructField("l_shipmode", T.StringType()),   T.StructField("l_comment", T.StringType()),
])

lineitem = (spark.read.option("sep","|").option("header",False).schema(lineitem_schema).csv("data/tpch/lineitem.tbl"))

#lineitem.show(5)

# Task 1
# Compute revenue = l_extendedprice * (1 - l_discount) and charge = revenue * (1 + l_tax).
#lineitem.withColumn("revenue",F.col("l_extendedprice") * (1 - F.col("l_discount"))).select("l_extendedprice","l_discount","revenue").show()
lineitem=lineitem.withColumn("revenue",F.col("l_extendedprice") * (1 - F.col("l_discount")))


#lineitem.withColumn("charge",F.col("revenue")* (1+ F.col("l_tax"))).select("charge","revenue","l_tax").show()


# Task 2

# Compute ship_delay_days = datediff(l_receiptdate, l_shipdate) and late_flag with F.when.

lineitem=lineitem.withColumn("ship_delay_days",F.datediff(F.col("l_receiptdate") , F.col("l_shipdate")))
lineitem.withColumn("late_flag",F.when(F.col("ship_delay_days")>0,"LATE").otherwise("Not late")).show(10)


# Task 4
# Filter to valid lines (positive quantity, non-negative discount, sensible dates) and count rows removed.


lineitem_filtered=lineitem.filter((F.col("l_quantity")>0) & (F.col("l_discount")>=0) & (F.col("l_receiptdate").isNotNull()) & 
                (F.col("l_shipdate").isNotNull()) &  (F.col("l_receiptdate") >= F.col("l_shipdate")))
valid_lines= abs( lineitem_filtered.count()-lineitem.count())
#print(valid_lines)



#order.show(9)
#  Extract order_year and order_month from o_orderdate.

#order.select(F.month("o_orderdate").alias("Date"),F.year("o_orderdate").alias("Year"),
             #F.col("o_orderdate").alias("Order Date")).show(5)


# order.withColumn("price_bucket",
#                   F.when(F.col("o_totalprice") < 30000, "small")
#      .when(F.col("o_totalprice") < 100000, "medium")
#      .otherwise("large")).show(7)


testfile= spark.read.csv("data/tpch/testfile.tbl",sep="|",inferSchema=False)
testfile.show()

# Task 6
# Handle nulls in at least one column with both fillna and dropna; note the row-count difference.

testfile_after= testfile.fillna({"_c1":"ok"})
testfile_afterd= testfile.dropna(subset="_c0")

print(testfile_after.count(), testfile_afterd.count())

testfile_after.show()