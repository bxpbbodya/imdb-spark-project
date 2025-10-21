from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, DecisionTreeClassifier
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator

def prepare_data(df):
    df = df.dropna(subset=["runtimeMinutes", "startYear", "isAdult"])
    assembler = VectorAssembler(inputCols=["startYear"], outputCol="features")
    data = assembler.transform(df).select("features", "runtimeMinutes", "isAdult")
    return data


def regression_models(data):
    print("\nРегресійні моделі")
    lr = LinearRegression(featuresCol="features", labelCol="runtimeMinutes")
    lr_model = lr.fit(data)
    lr_preds = lr_model.transform(data)
    eval = RegressionEvaluator(labelCol="runtimeMinutes")
    print("Linear Regression → RMSE:", eval.evaluate(lr_preds, {eval.metricName: "rmse"}))
    print("                    → R²:", eval.evaluate(lr_preds, {eval.metricName: "r2"}))

    dtr = DecisionTreeRegressor(featuresCol="features", labelCol="runtimeMinutes")
    dtr_model = dtr.fit(data)
    dtr_preds = dtr_model.transform(data)
    print("DecisionTreeRegressor → RMSE:", eval.evaluate(dtr_preds, {eval.metricName: "rmse"}))
    print("                         → R²:", eval.evaluate(dtr_preds, {eval.metricName: "r2"}))


def classification_models(data):
    print("\nКласифікаційні моделі")
    logreg = LogisticRegression(labelCol="isAdult", featuresCol="features")
    log_model = logreg.fit(data)
    log_preds = log_model.transform(data)
    eval = MulticlassClassificationEvaluator(labelCol="isAdult", predictionCol="prediction")
    print("Logistic Regression → Accuracy:", eval.evaluate(log_preds, {eval.metricName: "accuracy"}))
    print("                       → F1:", eval.evaluate(log_preds, {eval.metricName: "f1"}))
    print("                       → Precision:", eval.evaluate(log_preds, {eval.metricName: "weightedPrecision"}))
    print("                       → Recall:", eval.evaluate(log_preds, {eval.metricName: "weightedRecall"}))

    rf = RandomForestClassifier(labelCol="isAdult", featuresCol="features")
    rf_model = rf.fit(data)
    rf_preds = rf_model.transform(data)
    print("RandomForestClassifier → Accuracy:", eval.evaluate(rf_preds, {eval.metricName: "accuracy"}))
    print("                          → F1:", eval.evaluate(rf_preds, {eval.metricName: "f1"}))
    print("                          → Precision:", eval.evaluate(rf_preds, {eval.metricName: "weightedPrecision"}))
    print("                          → Recall:", eval.evaluate(rf_preds, {eval.metricName: "weightedRecall"}))

    dt = DecisionTreeClassifier(labelCol="isAdult", featuresCol="features")
    dt_model = dt.fit(data)
    dt_preds = dt_model.transform(data)
    print("DecisionTreeClassifier → Accuracy:", eval.evaluate(dt_preds, {eval.metricName: "accuracy"}))
    print("                         → F1:", eval.evaluate(dt_preds, {eval.metricName: "f1"}))
    print("                         → Precision:", eval.evaluate(dt_preds, {eval.metricName: "weightedPrecision"}))
    print("                         → Recall:", eval.evaluate(dt_preds, {eval.metricName: "weightedRecall"}))
