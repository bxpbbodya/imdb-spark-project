from pyspark.sql import functions as F
from pyspark.sql.window import Window


# 🧭 1️⃣ Базова інформація про датасет
def dataset_info(df):
    print("📊 Кількість рядків:", df.count())
    print("📋 Кількість колонок:", len(df.columns))
    df.printSchema()


# 🔢 2️⃣ Статистика по числових стовпцях
def numeric_stats(df):
    print("📈 Статистика по числових колонках:")
    df.select("startYear", "runtimeMinutes", "isAdult").describe().show()


# 💼 3️⃣ Бізнес-запити
def business_queries(df):
    print("\n=== 1. Фільмів у кожному жанрі (groupBy + count) ===")
    df.groupBy("genres").count().orderBy(F.desc("count")).show(10)

    print("\n=== 2. Кількість фільмів по роках (groupBy + count) ===")
    df.groupBy("startYear").count().orderBy(F.desc("startYear")).show(10)

    print("\n=== 3. Топ-10 найдовших фільмів (filter + orderBy) ===")
    df.filter(F.col("runtimeMinutes") > 0) \
      .orderBy(F.desc("runtimeMinutes")) \
      .select("primaryTitle", "runtimeMinutes") \
      .show(10)

    print("\n=== 4. Фільми для дорослих (filter) ===")
    df.filter(F.col("isAdult") == 1).select("primaryTitle", "startYear").show(5)

    print("\n=== 5. Короткі фільми < 10 хв (filter + count) ===")
    short_count = df.filter(F.col("runtimeMinutes") < 10).count()
    print("Кількість коротких фільмів:", short_count)

    print("\n=== 6. Середня тривалість за жанрами (groupBy + avg) ===")
    df.groupBy("genres") \
      .agg(F.avg("runtimeMinutes").alias("avg_length")) \
      .orderBy(F.desc("avg_length")) \
      .show(10)


# 🔄 4️⃣ Приклад JOIN (імітація для IMDB)
def join_examples(df):
    # Створимо умовний датафрейм з рейтингами (імітація іншого IMDB файлу)
    ratings_data = [
        ("tt0000001", 5.6, 200),
        ("tt0000002", 6.0, 180),
        ("tt0000003", 7.3, 250),
        ("tt0000004", 5.9, 100),
        ("tt0000005", 6.4, 150)
    ]
    ratings_df = df.sparkSession.createDataFrame(ratings_data, ["tconst", "averageRating", "numVotes"])

    print("\n=== 7. Join із рейтингами (inner join) ===")
    joined = df.join(ratings_df, on="tconst", how="inner")
    joined.select("primaryTitle", "averageRating", "numVotes").show(5)

    print("\n=== 8. Join для пошуку фільмів > 6.0 рейтингу ===")
    joined.filter(F.col("averageRating") > 6.0).select("primaryTitle", "averageRating").show(5)


# 🪟 5️⃣ Приклад WINDOW функцій
def window_examples(df):
    window_spec = Window.partitionBy("genres").orderBy(F.desc("runtimeMinutes"))

    print("\n=== 9. Top-1 найдовший фільм у кожному жанрі (window) ===")
    df.withColumn("rank", F.row_number().over(window_spec)) \
      .filter(F.col("rank") == 1) \
      .select("genres", "primaryTitle", "runtimeMinutes") \
      .orderBy(F.desc("runtimeMinutes")) \
      .show(10)

    print("\n=== 10. Середня тривалість по жанрах (window avg) ===")
    df.withColumn("avg_len_genre", F.avg("runtimeMinutes").over(Window.partitionBy("genres"))) \
      .select("genres", "primaryTitle", "runtimeMinutes", "avg_len_genre") \
      .show(10)


import os

def save_results(df, path="output/results.csv"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pdf = df.limit(1000).toPandas()  # збережемо лише частину, щоб не перевантажити RAM
    pdf.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"✅ Результати збережено у {os.path.abspath(path)}")

