import pyspark.sql.types as T

customer_schema = T.StructType([
    T.StructField("c_custkey", T.LongType()),
    T.StructField("c_name", T.StringType()),
    T.StructField("c_address", T.StringType()),
    T.StructField("c_nationkey", T.IntegerType()),
    T.StructField("c_phone", T.StringType()),
    T.StructField("c_acctbal", T.DoubleType()),
    T.StructField("c_mktsegment", T.StringType()),
    T.StructField("c_comment", T.StringType()),
])


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



