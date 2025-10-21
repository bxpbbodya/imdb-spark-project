from pyspark.sql import SparkSession
from modules.extract import load_imdb_dataset

spark = SparkSession.builder.appName("IMDB Spark Project").master("local[*]").getOrCreate()
df = load_imdb_dataset(spark, r"D:\Coding\Projects\BigVidob\data\title.basics.tsv")

df.show(5)
print("Кількість рядків:", df.count())
