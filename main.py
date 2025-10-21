import os
import subprocess
from pyspark.sql import SparkSession
from modules.extract import load_imdb_dataset
from modules.transform import (
    dataset_info, numeric_stats,
    business_queries, join_examples,
    window_examples, save_results
)
from modules.analysis import (
    prepare_data,
    regression_models,
    classification_models
)

os.environ["PYSPARK_PYTHON"] = r"D:\Coding\Projects\BigVidob\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Coding\Projects\BigVidob\.venv\Scripts\python.exe"

# Налаштування середовища
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-21.0.8.9-hotspot"
os.environ["SPARK_HOME"] = r"C:\spark"
os.environ["HADOOP_HOME"] = r"C:\spark"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["SPARK_HOME"], "bin")
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["JAVA_HOME"], "bin")

# Перевірка Java
try:
    subprocess.run(["java", "-version"], check=True)
except Exception as e:
    print(" Java не знайдена:", e)
    exit(1)

spark = SparkSession.builder.appName("IMDB Spark Project").master("local[*]").getOrCreate()

# Зчитування датасету
df = load_imdb_dataset(spark, r"D:\Coding\Projects\BigVidob\data\title.basics.tsv")

# Етап трансформацій
dataset_info(df)
numeric_stats(df)
business_queries(df)
join_examples(df)
window_examples(df)
save_results(df)

# Етап машинного навчання
print("\n===  Етап аналізу даних (ML) ===")
data = prepare_data(df)
regression_models(data)
classification_models(data)

print("\nУсі етапи виконані успішно!")

spark.stop()

print("\nВисновок:")
print("• DecisionTreeRegressor трохи точніший за LinearRegression (вищий R²).")
print("• RandomForestClassifier має найкращий баланс між Precision і Recall.")
print("• Runtime майже не залежить від року, класифікація працює стабільно.")