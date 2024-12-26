from pyspark.sql import SparkSession

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("CityAverageBalance") \
    .getOrCreate()

user_balance_path = "/home/liuziyi/bigdata/user_balance_table.csv"
user_profile_path = "/home/liuziyi/bigdata/user_profile_table.csv"

# 加载 CSV 文件为 DataFrame
user_balance_df = spark.read.csv(user_balance_path, header=True, inferSchema=True)
user_profile_df = spark.read.csv(user_profile_path, header=True, inferSchema=True)

user_balance_df.createOrReplaceTempView("user_balance_table")
user_profile_df.createOrReplaceTempView("user_profile_table")

query = """
    SELECT 
        up.city AS city,
        ROUND(AVG(ub.tBalance), 2) AS avg_balance
    FROM 
        user_balance_table ub
    JOIN 
        user_profile_table up
    ON 
        ub.user_id = up.user_id
    WHERE 
        ub.report_date = '20140301'
    GROUP BY 
        up.city
    ORDER BY 
        avg_balance DESC
"""

result_df = spark.sql(query)

# 输出到终端
result_df.show(truncate=False)

output_path = "/home/liuziyi/wxw/CityAverageBalance.csv"
result_df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

spark.stop()