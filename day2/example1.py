from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    StringType,
    DateType,
    DoubleType
)

# import pyspark.sql.types as T : by doing this can i just do T.StringType ..

spark = (SparkSession.builder
         .appName("sparkexample2")
         .master("local[*]")
         .config("spark.driver.memory","2g")

         .getOrCreate()
         
         )

print(spark.version)

customer_schema = StructType([
    StructField("c_custkey", LongType()),
    StructField("c_name", StringType()),
    StructField("c_address", StringType()),
    StructField("c_nationkey", IntegerType()),
    # StructField("c_phone", StringType()),
    # StructField("c_acctbal", DoubleType()),
    # StructField("c_mktsegment", StringType()),
    # StructField("c_comment", StringType()),
])

print(type(customer_schema))

#df= spark.read.csv("data/tpch/customer.tbl", sep="|")
#df.show(8)

customer = (
    spark.read
    .option("sep", "|")
    #so if gave the header as true..spark think..the first row as header and ignore that.
    .option("header", False)
    .schema(customer_schema)
    .csv("data/tpch/customer.tbl")
)

customer.printSchema()

#customer.show(7)

order_schema = StructType([
    StructField("o_orderkey", LongType()),
    StructField("o_custkey", LongType()),
    StructField("o_orderstatus", StringType()),
    StructField("o_totalprice", DoubleType()),
    StructField("o_orderdate", DateType()),
    StructField("o_orderpriority", StringType()),
    StructField("o_clerk", StringType()),
    StructField("o_shippriority", IntegerType()),
    StructField("o_comment", StringType()),
])

orders = (
    spark.read
    .option("sep", "|")
    .option("header", False)
    .schema(order_schema)
    .csv("data/tpch/orders.tbl")
)

#orders.show(5, truncate=False)
#orders.printSchema()




lineitem_schema = StructType([
    StructField("l_orderkey", LongType()),    StructField("l_partkey", LongType()),
    StructField("l_suppkey", LongType()),     StructField("l_linenumber", IntegerType()),
    StructField("l_quantity", DoubleType()),  StructField("l_extendedprice", DoubleType()),
    StructField("l_discount", DoubleType()),  StructField("l_tax", DoubleType()),
    StructField("l_returnflag", StringType()),StructField("l_linestatus", StringType()),
    StructField("l_shipdate", DateType()),     StructField("l_commitdate", DateType()),
    StructField("l_receiptdate", DateType()),  StructField("l_shipinstruct", StringType()),
    StructField("l_shipmode", StringType()),   StructField("l_comment", StringType()),
])
lineitem = (spark.read
            .option("sep", "|")
            .schema(lineitem_schema)
            .option("header", False)
            .csv("data/tpch/lineitem.tbl"))

lineitem.show(5)

nation_schema= StructType([
    StructField("n_key", IntegerType()),
    StructField("n_name", StringType()),
    StructField("n_rep", StringType()),
    StructField("n_comment",StringType())
])

nation= (spark.read.option("sep","|")
         .option("header", False)
         .schema(nation_schema)
         .csv("data/tpch/nation.tbl"))

#nation.show(7)


region_schema= StructType(
    [
        StructField("r_number", IntegerType()),
        StructField("r_name", StringType()),
        StructField("r_comment", StringType())
    ]
)

region= (spark.read.option("sep","|")
         .option("header",False)
         .schema(region_schema)
         .csv("data/tpch/region.tbl"))

region.show()