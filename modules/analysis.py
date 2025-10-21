from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator

def prepare_data(df):
    df = df.dropna(subset=["runtimeMinutes", "startYear", "isAdult"])
    assembler = VectorAssembler(inputCols=["startYear"], outputCol="features")
    data = assembler.transform(df).select("features", "runtimeMinutes", "isAdult")
    return data

def regression_model(data):
    lr = LinearRegression(featuresCol="features", labelCol="runtimeMinutes")
    model = lr.fit(data)
    preds = model.transform(data)
    eval = RegressionEvaluator(labelCol="runtimeMinutes")
    print("RMSE:", eval.evaluate(preds, {eval.metricName: "rmse"}))
    print("R²:", eval.evaluate(preds, {eval.metricName: "r2"}))

def classification_model(data):
    rf = RandomForestClassifier(labelCol="isAdult", featuresCol="features")
    model = rf.fit(data)
    preds = model.transform(data)
    eval = MulticlassClassificationEvaluator(labelCol="isAdult", predictionCol="prediction")
    print("Accuracy:", eval.evaluate(preds, {eval.metricName: "accuracy"}))
