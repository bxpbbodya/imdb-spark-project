import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

# 🔧 Налаштування середовища
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-21"
os.environ["PYSPARK_PYTHON"] = r"D:\Coding\Projects\BigVidob\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Coding\Projects\BigVidob\.venv\Scripts\python.exe"

# Використовуємо всі локальні ядра
spark = SparkSession.builder \
    .appName("IMDB Spark Project") \
    .config("spark.driver.host", "127.0.0.1") \
    .master("local[*]") \
    .getOrCreate()

# 🔹 Схема
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

# 🔹 Зчитування TSV
df = spark.read.csv(
    r"D:\Datasets\imdb\title.basics.tsv",
    sep="\t",
    header=True,
    schema=schema,
    enforceSchema=True   # Примусово застосовуємо схему
)

# 🔹 Перетворення рядкових чисел у Integer (якщо потрібно)
df = df.withColumn("startYear", col("startYear").cast("integer")) \
       .withColumn("endYear", col("endYear").cast("integer")) \
       .withColumn("runtimeMinutes", col("runtimeMinutes").cast("integer"))

# 🔹 Перевірка
try:
    df.show(5)
    df.printSchema()
    print("Кількість рядків:", df.count())
except Exception as e:
    print("Помилка при роботі з DataFrame:", e)

spark.stop()
