from pyspark.sql.functions import col, count, avg, desc

def dataset_info(df):
    print("Кількість рядків:", df.count())
    print("Кількість колонок:", len(df.columns))
    df.printSchema()

def numeric_stats(df):
    df.select("startYear", "runtimeMinutes", "isAdult").describe().show()

def business_queries(df):
    # 1. Скільки фільмів з кожного жанру
    df.groupBy("genres").count().orderBy(desc("count")).show(10)

    # 2. Скільки фільмів виходило кожного року
    df.groupBy("startYear").count().orderBy(desc("startYear")).show(10)

    # 3. Топ-10 найдовших фільмів
    df.orderBy(desc("runtimeMinutes")).select("primaryTitle", "runtimeMinutes").show(10)

    # 4. Фільми для дорослих
    df.filter(col("isAdult") == 1).select("primaryTitle", "startYear").show(5)

    # 5. Короткі фільми (менше 10 хв)
    df.filter(col("runtimeMinutes") < 10).count()

    # 6. Середня тривалість фільмів за жанрами
    df.groupBy("genres").agg(avg("runtimeMinutes").alias("avg_length")).orderBy(desc("avg_length")).show(10)

def save_results(df, path="output/"):
    df.write.csv(path + "results.csv", header=True, mode="overwrite")
