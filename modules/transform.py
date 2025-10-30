from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os



# === 1️⃣ Базова інформація про датасет ===
def dataset_info(df):
    print("Кількість рядків:", df.count())
    print("Кількість колонок:", len(df.columns))
    df.printSchema()


# === 2️⃣ Статистика по числових стовпцях ===
def numeric_stats(df):
    print("\n=== Статистика по числових колонках ===")
    df.select("startYear", "runtimeMinutes", "isAdult").describe().show()

def clean_dataset(df):
    """Очищення даних від аномалій і нульових значень."""
    print("\n=== Очищення датасету ===")

    # Обмежуємо роки випуску
    df = df.filter((F.col("startYear") >= 1900) & (F.col("startYear") <= 2030))

    # Прибираємо аномальні або короткі фільми (< 20 хв)
    df = df.filter((F.col("runtimeMinutes") >= 20) & (F.col("runtimeMinutes") <= 10000))

    # Прибираємо фільми без жанру або з null
    df = df.filter(F.col("genres").isNotNull())

    # Прибираємо фільми без рейтингу або голосів
    if "ratings_avgRating" in df.columns:
        df = df.filter(F.col("ratings_avgRating").isNotNull())

    # Видаляємо дублікатні tconst
    df = df.dropDuplicates(["tconst"])

    print(f"Після очищення: {df.count()} рядків, {len(df.columns)} колонок")
    return df


# === 3️⃣ Бізнес-запити ===
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def business_queries(df, akas, ratings):
    # ✅ Очищення перед аналітикою
    df = df.dropDuplicates(["tconst"])
    df = df.filter(
        (F.col("startYear") >= 1900) & (F.col("startYear") <= 2030) &
        (F.col("runtimeMinutes") >= 40) & (F.col("runtimeMinutes") <= 10000)
    )

    print("\n=== 1. Кількість фільмів у кожному жанрі ===")
    df.groupBy("genres").count().orderBy(F.desc("count")).show(10, truncate=False)

    print("\n=== 2. Кількість фільмів по роках ===")
    df.groupBy("startYear").count().orderBy(F.desc("startYear")).show(10)

    print("\n=== 3. Топ-10 найдовших фільмів ===")
    long_films = df.filter(F.col("runtimeMinutes") > 100) \
                   .orderBy(F.desc("runtimeMinutes")) \
                   .select("primaryTitle", "runtimeMinutes") \
                   .distinct()
    if long_films.count() == 0:
        long_films = df.orderBy(F.desc("runtimeMinutes")) \
                       .select("primaryTitle", "runtimeMinutes") \
                       .distinct()
    long_films.show(10, truncate=False)

    print("\n=== 4. Фільми для дорослих ===")
    df.filter(F.col("isAdult") == 1) \
      .select("primaryTitle", "startYear") \
      .distinct().show(5, truncate=False)

    print("\n=== 5. ТОП-10 драм 2000–2019 з рейтингом >8 і 5000+ голосів ===")
    top_dramas = df.filter(
        (F.col("startYear").between(2000, 2019)) &
        (F.col("ratings_avgRating") > 8.0) &
        (F.col("ratings_numVotes") > 5000) &
        (F.col("genres").like("%Drama%"))
    ).select(
        "primaryTitle", "genres", "ratings_avgRating", "ratings_numVotes", "startYear"
    ).orderBy(F.desc("ratings_avgRating"), F.desc("ratings_numVotes"))

    if top_dramas.count() == 0:
        print("⚠️ Немає фільмів, що відповідають умові.")
    else:
        top_dramas.show(10, truncate=False)

    print("\n=== 6. Середній рейтинг по жанрах (2000–2019, 5000+ голосів) ===")
    avg_genres = df.filter(
        (F.col("startYear").between(2000, 2019)) &
        (F.col("ratings_numVotes") > 5000) &
        (F.col("ratings_avgRating").isNotNull())
    ).groupBy("genres") \
        .agg(F.round(F.avg("ratings_avgRating"), 2).alias("avg_rating")) \
        .orderBy(F.desc("avg_rating"))

    if avg_genres.count() == 0:
        print("⚠️ Немає достатньо даних для підрахунку середніх рейтингів.")
    else:
        avg_genres.show(10, truncate=False)

    # ✅ === 7. Топ-10 фільмів з найбільшою кількістю перекладів ===
    print("\n=== 7. Топ-10 фільмів з найбільшою кількістю перекладів ===")

    top_translated = (
        df.alias("b")
        .join(akas.alias("a"), F.col("b.tconst") == F.col("a.titleId"), "inner")
        .join(ratings.alias("r"), F.col("b.tconst") == F.col("r.tconst"), "inner")
        .groupBy("b.primaryTitle", "b.startYear", "r.averageRating")
        .agg(F.count("a.title").alias("translation_count"))
        .orderBy(F.desc("translation_count"))
        .limit(10)
    )

    if top_translated.count() == 0:
        print("⚠️ Немає даних про переклади.")
    else:
        top_translated.show(truncate=False)

    # ✅ === 8. ТОП-3 фільми в кожному жанрі за рейтингом (з перекладами) ===
    print("\n=== 8. ТОП-3 фільми в кожному жанрі за рейтингом (з перекладами) ===")

    w = Window.partitionBy("b.genres").orderBy(F.desc("r.averageRating"))

    top3_genres = (
        df.alias("b")
        .join(akas.alias("a"), F.col("b.tconst") == F.col("a.titleId"), "inner")
        .join(ratings.alias("r"), F.col("b.tconst") == F.col("r.tconst"), "inner")
        .filter(F.col("r.numVotes") > 5000)
        .select(
            "b.primaryTitle",
            "b.genres",
            "a.region",
            "r.averageRating",
            F.row_number().over(w).alias("rank")
        )
        .filter(F.col("rank") <= 3)
        .orderBy("b.genres", "rank")
    )

    if top3_genres.count() == 0:
        print("⚠️ Недостатньо даних для вибірки.")
    else:
        top3_genres.show(50, truncate=False)



def join_examples(df):
    # Створюємо тестовий DataFrame з існуючими tconst (щоб join точно спрацював)
    example_ids = [r["tconst"] for r in df.limit(5).collect()]
    ratings_data = [(t, 5.5 + i * 0.2, 100 + i * 50) for i, t in enumerate(example_ids)]
    ratings_df = df.sparkSession.createDataFrame(ratings_data, ["tconst", "avg", "votes"])

    print("\n=== 7. Join із тестовими рейтингами ===")
    joined = df.join(ratings_df, on="tconst", how="inner")
    if joined.count() == 0:
        print("⚠️ Не вдалося знайти збіги за tconst — можливо, DataFrame пустий.")
    else:
        joined.select("primaryTitle", "avg", "votes").distinct().show(5, truncate=False)

    print("\n=== 8. Фільми з рейтингом > 6.0 (тестовий join) ===")
    joined.filter(F.col("avg") > 6.0).select("primaryTitle", "avg").distinct().show(5, truncate=False)


# === 5️⃣ WINDOW функції ===
def window_examples(df):
    df = df.dropDuplicates(["tconst"])
    window_spec = Window.partitionBy("genres").orderBy(F.desc("runtimeMinutes"))

    print("\n=== 9. Найдовший фільм у кожному жанрі ===")
    df.withColumn("rank", F.row_number().over(window_spec)) \
      .filter(F.col("rank") == 1) \
      .select("genres", "primaryTitle", "runtimeMinutes") \
      .orderBy(F.desc("runtimeMinutes")) \
      .show(10, truncate=False)

    print("\n=== 10. Середня тривалість у жанрі (window avg) ===")
    df.withColumn("avg_len_genre", F.avg("runtimeMinutes").over(Window.partitionBy("genres"))) \
      .select("genres", "primaryTitle", "runtimeMinutes", "avg_len_genre") \
      .show(10, truncate=False)


# === 6️⃣ Збереження результатів ===
def save_results(df, path="output/results.csv"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pdf = df.dropDuplicates(["tconst"]).limit(1000).toPandas()
    pdf.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"\nРезультати збережено у {os.path.abspath(path)}")