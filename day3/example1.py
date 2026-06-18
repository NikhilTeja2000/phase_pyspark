from pyspark.sql import SparkSession , functions as F
import pyspark.sql.types as T



spark = (SparkSession.builder.appName("mike").master("local[*]").config("spark.driver.memory","2g").getOrCreate())


print(spark.version)

region=spark.read.csv("data/tpch/region.tbl",sep="|",header=False)

region.show()

region_name=region.select("_c1")
region_name.show()

national= spark.read.csv("data/tpch/nation.tbl",sep="|", header= False)

#national.filter("_c2 > 3").show()
#national.filter(F.col("_c2")>2).show()
#national.withColumn("rank" , F.col("_c2")).show()
national=national.withColumnRenamed( "_c2","hype")

#national.drop("_c4").show()
#region.select(F.col("_c1").alias("region_name")).show()

#region.withColumn("country", F.lit("global")).show()

#region.select(F.expr("_c0 + 100").alias("new_id")).show()


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

customer= (spark.read.option("sep","|")
           .option("header",False)
           .schema(customer_schema)
           .csv("data/tpch/customer.tbl"))

customer.filter(F.col("c_acctbal")>1000).show()
customer.where((F.col("c_mktsegment")== "AUTOMOBILE") & (F.col("c_nationkey")>10)).show(5)
customer.withColumn(
    "balance_category",
    F.when(F.col("c_acctbal") > 5000, "HIGH")
     .otherwise("LOW")
).show(5)



