from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = SparkConf().setAppName("Filtering Movies")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
df = spark.read.csv("s3://data-bucket-22092000/monthly-build/03-2024/input/comedy_serigies.csv", header=True, inferSchema=True)
drama_ = df.filter(df.Genre.contains("Drama"))
movies_ = df.filter(df['Release Year'] > 2000)
drama_.createOrReplaceTempView("drama")
movies_.createOrReplaceTempView("movies")
drama_.write.mode("overwrite").csv("s3://data-bucket-22092000/monthly-build/03-2024/output/drama.csv")
movies_.write.mode("overwrite").csv("s3://data-bucket-22092000/monthly-build/03-2024/output/movies.csv")
sc.stop()