from pyspark.sql import SparkSession

spark =(SparkSession.builder
        .appName("learning-pyspark")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
        )
print("spark version: ", spark.version)