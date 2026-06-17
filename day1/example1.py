from pyspark.sql import SparkSession

spark =(SparkSession.builder
        .appName("learning-pyspark")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
        )
print("spark version: ", spark.version)
#so if the default infer schema is false and it considered all as string then we can set it to true and it will try to infer the schema based on the data
df = spark.read.csv("data/tpch/customer.tbl", sep="|", inferSchema=True, header=False)
orders = spark.read.csv("data/tpch/orders.tbl", sep="|", inferSchema=True, header=False)
df.show(5)
df.printSchema()

orders.show(5,truncate=False)
orders.printSchema()
print("count of customer table: ", df.count())
print("count of orders table: ", orders.count())

print("number of partitions in customer table: ", df.rdd.getNumPartitions())
print("number of partitions in orders table: ", orders.rdd.getNumPartitions())
