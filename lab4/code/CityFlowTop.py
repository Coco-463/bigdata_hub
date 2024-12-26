from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CityTop3Traffic") \
    .getOrCreate()

user_balance_path = "/home/liuziyi/bigdata/user_balance_table.csv"
user_profile_path = "/home/liuziyi/bigdata/user_profile_table.csv"

# 加载 CSV 文件为 DataFrame
user_balance_df = spark.read.csv(user_balance_path, header=True, inferSchema=True)
user_profile_df = spark.read.csv(user_profile_path, header=True, inferSchema=True)

user_balance_df.createOrReplaceTempView("user_balance_table")
user_profile_df.createOrReplaceTempView("user_profile_table")

query = """
    WITH UserMonthlyFlow AS (
        SELECT
            ub.user_id,
            up.city AS city,
            SUM(ub.total_purchase_amt + ub.total_redeem_amt) AS total_flow
        FROM
            user_balance_table ub
        JOIN
            user_profile_table up
        ON
            ub.user_id = up.user_id
        WHERE
            ub.report_date BETWEEN 20140801 AND 20140831
        GROUP BY
            ub.user_id, up.city
    ),
    CityRankedFlow AS (
        SELECT
            city,
            user_id,
            total_flow,
            ROW_NUMBER() OVER (PARTITION BY city ORDER BY total_flow DESC) AS rank
        FROM
            UserMonthlyFlow
    )
    SELECT
        city,
        user_id,
        total_flow
    FROM
        CityRankedFlow
    WHERE
        rank <= 3
    ORDER BY
        city, rank
"""

result_df = spark.sql(query)

# 输出到终端
result_df.show(truncate=False)

output_path = "/home/liuziyi/bigdata/CityTop3Traffic.csv"
result_df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

spark.stop()