from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("ActiveUserAnalysis")
sc = SparkContext(conf=conf)

data_path = "/home/liuziyi/bigdata/user_balance_table.csv" 
raw_data = sc.textFile(data_path)

header = raw_data.first()
data = raw_data.filter(lambda line: line != header)

parsed_rdd = data.map(lambda line: line.split(","))

filtered_rdd = parsed_rdd.filter(lambda x: x[1][:6] == "201408")

# 按用户 ID 分组日期
user_active_days = filtered_rdd.map(lambda x: (x[0], x[1])).distinct() \
                               .groupByKey() \
                               .mapValues(len)

active_users = user_active_days.filter(lambda x: x[1] >= 5)

# 统计活跃用户总数
active_user_count = active_users.count()

print(f"< {active_user_count} >")

output_path = "/home/liuziyi/wxw/FundInOut.csv/ActiveUser_out"
sc.parallelize([f"< {active_user_count} >"]).coalesce(1).saveAsTextFile(output_path)

sc.stop()