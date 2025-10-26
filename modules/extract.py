from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql import functions as F


# === 1. title.basics.tsv ===
def load_basics(spark, path):
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

    df = spark.read.csv(path, sep="\t", header=True, schema=schema, nullValue="\\N")
    print(f"\n✅ basics завантажено: {df.count()} рядків, {len(df.columns)} колонок")
    return df


# === 2. title.akas.tsv ===
def load_akas(spark, path):
    schema = StructType([
        StructField("titleId", StringType(), True),
        StructField("ordering", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("region", StringType(), True),
        StructField("language", StringType(), True),
        StructField("types", StringType(), True),
        StructField("attributes", StringType(), True),
        StructField("isOriginalTitle", IntegerType(), True)
    ])

    df = spark.read.csv(path, sep="\t", header=True, schema=schema, nullValue="\\N")
    print(f"✅ akas завантажено: {df.count()} рядків, {len(df.columns)} колонок")
    return df


# === 3. title.ratings.tsv ===
def load_ratings(spark, path):
    schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("averageRating", DoubleType(), True),
        StructField("numVotes", IntegerType(), True)
    ])

    df = spark.read.csv(path, sep="\t", header=True, schema=schema, nullValue="\\N")
    print(f"✅ ratings завантажено: {df.count()} рядків, {len(df.columns)} колонок")

    # Фільтрація порожніх або некоректних значень
    df = df.filter((F.col("averageRating").isNotNull()) & (F.col("numVotes") > 0))
    return df
