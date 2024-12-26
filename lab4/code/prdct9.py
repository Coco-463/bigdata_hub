from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql.functions import col, substring
import pandas as pd
from pyspark.sql.functions import col, substring, format_number
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.regression import DecisionTreeRegressor

# 初始化 SparkConf 和 SparkContext
conf = SparkConf().setAppName("DailyInOutAnalysis").set("spark.eventLog.enabled", "false")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

file_path = "/home/liuziyi/wxw/FundInOut.csv"  # 替换为实际文件路径

df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

# 提取日期的年、月、日作为特征
df = df.withColumn("year", substring("date", 1, 4).cast("int"))
df = df.withColumn("month", substring("date", 5, 2).cast("int"))
df = df.withColumn("day", substring("date", 7, 2).cast("int"))

# 选择特征列（年、月、日）和目标变量（inflow, outflow）
df = df.select("year", "month", "day", "inflow", "outflow")

# 组合特征为向量
assembler = VectorAssembler(inputCols=["year", "month", "day"], outputCol="features")
df = assembler.transform(df)

# 使用 DecisionTreeRegressor 替换 RandomForestRegressor
inflow_dt_model = DecisionTreeRegressor(featuresCol="features", labelCol="inflow")
inflow_dt_model_trained = inflow_dt_model.fit(df)

outflow_dt_model = DecisionTreeRegressor(featuresCol="features", labelCol="outflow")
outflow_dt_model_trained = outflow_dt_model.fit(df)

# 生成2014年9月的日期
september_dates = [f"201409{day:02d}" for day in range(1, 31)]

prediction_df = pd.DataFrame(september_dates, columns=["date"])

# 转换为 Spark DataFrame
prediction_spark_df = spark.createDataFrame(prediction_df)

# 提取年、月、日
prediction_spark_df = prediction_spark_df.withColumn("year", substring("date", 1, 4).cast("int"))
prediction_spark_df = prediction_spark_df.withColumn("month", substring("date", 5, 2).cast("int"))
prediction_spark_df = prediction_spark_df.withColumn("day", substring("date", 7, 2).cast("int"))

# 合并特征列
prediction_spark_df = assembler.transform(prediction_spark_df)

# 预测
inflow_predictions = inflow_dt_model_trained.transform(prediction_spark_df)
outflow_predictions = outflow_dt_model_trained.transform(prediction_spark_df)

# 提取预测结果
predictions = inflow_predictions.select("date", "prediction").withColumnRenamed("prediction", "inflow")
predictions = predictions.join(outflow_predictions.select("date", "prediction").withColumnRenamed("prediction", "outflow"), on="date")

# 转换日期列为BIGINT类型
predictions = predictions.withColumn("date", col("date").cast("bigint"))

# 保证 inflow 和 outflow 为整数，精确到元（不使用科学计数法）
predictions = predictions.withColumn("inflow", col("inflow").cast("bigint"))
predictions = predictions.withColumn("outflow", col("outflow").cast("bigint"))

# 显示预测结果
predictions.show(35)

output_path = "/home/liuziyi/wxw/predict_output_dt.csv"
predictions.coalesce(1).write.option("header", "false").mode("overwrite").csv(output_path)

sc.stop()