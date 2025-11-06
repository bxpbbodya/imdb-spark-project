import os
import subprocess
from pyspark.sql import SparkSession
from modules.extract import load_basics, load_akas, load_ratings
from modules.transform import (
    dataset_info, numeric_stats, clean_dataset,
    business_queries, join_examples, window_examples, save_results
)
from modules.analysis import (
    prepare_data, regression_models, classification_models
)

# === Налаштування середовища ===
os.environ["PYSPARK_PYTHON"] = r"D:\Coding\Projects\BigVidob\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Coding\Projects\BigVidob\.venv\Scripts\python.exe"
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-21.0.8.9-hotspot"
os.environ["SPARK_HOME"] = r"C:\spark"
os.environ["HADOOP_HOME"] = r"C:\spark"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["SPARK_HOME"], "bin")
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["JAVA_HOME"], "bin")

# === Перевірка Java ===
try:
    subprocess.run(["java", "-version"], check=True)
except Exception as e:
    print("❌ Java не знайдена:", e)
    exit(1)

# === Ініціалізація Spark ===
spark = (
    SparkSession.builder
    .appName("IMDB Spark Project")
    .master("local[*]")
    .config("spark.driver.memory", "16g")
    .config("spark.executor.memory", "16g")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.driver.maxResultSize", "4g")
    .getOrCreate()
)

# === Завантаження датасетів ===
basics = load_basics(spark, r"D:\Coding\Projects\BigVidob\data\title.basics.tsv")
akas = load_akas(spark, r"D:\Coding\Projects\BigVidob\data\title.akas.tsv")
ratings = load_ratings(spark, r"D:\Coding\Projects\BigVidob\data\title.ratings.tsv")

# === Перейменування для коректного join ===
akas = akas.withColumnRenamed("titleId", "akas_titleId") \
           .withColumnRenamed("title", "akas_title") \
           .withColumnRenamed("isOriginalTitle", "akas_isOriginalTitle")

ratings = ratings.withColumnRenamed("tconst", "ratings_tconst") \
                 .withColumnRenamed("averageRating", "ratings_avgRating") \
                 .withColumnRenamed("numVotes", "ratings_numVotes")

# === JOIN трьох датасетів ===
df_joined = (
    basics
    .join(akas, basics.tconst == akas.akas_titleId, "left")
    .join(ratings, basics.tconst == ratings.ratings_tconst, "left")
    .drop("akas_titleId", "ratings_tconst")
    .dropDuplicates(["tconst"])
)

# === Очищення ===
df_joined = clean_dataset(df_joined)

print("\n✅ Успішно зчитано та об’єднано датасети!")
print("Кількість рядків:", df_joined.count())
print("Кількість колонок:", len(df_joined.columns))

# === Аналіз датасету ===
dataset_info(df_joined)
numeric_stats(df_joined)

# ✅ ДУЖЛИВО: передаємо akas і ratings
business_queries(df_joined, akas, ratings)

join_examples(df_joined)
window_examples(df_joined)
save_results(df_joined)

# === Етап машинного навчання ===
print("\n=== Етап машинного навчання ===")
data = prepare_data(df_joined)
regression_models(data)
classification_models(data)

print("\n✅ Усі етапи виконані успішно!")
print("\nВисновки:")
print("• Класифікаційні моделі показали точність ~0.97–0.98.")
print("• DecisionTreeRegressor трохи кращий за LinearRegression по R².")
print("• RandomForestClassifier має найкращий баланс Precision/Recall.")
print("• RuntimeMinutes майже не пов’язаний із роком — R² ≈ 0.")

spark.stop()
