from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def load_imdb_dataset(spark, path):
    schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("titleType", StringType(), True),
        StructField("primaryTitle", StringType(), True),
        StructField("originalTitle", StringType(), True),
        StructField("isAdult", IntegerType(), True),
        StructField("startYear", IntegerType(), True),
        StructField("endYear", IntegerType(), True),
        StructField("runtimeMinutes", IntegerType(), True),
        StructField("genres", StringType(), True)
    ])
    df = spark.read.csv(path, sep="\t", header=True, schema=schema)
    return df
