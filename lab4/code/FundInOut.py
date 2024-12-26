from pyspark import SparkConf, SparkContext

# 初始化 SparkContext
conf = SparkConf().setAppName("FundFlowAnalysis")
sc = SparkContext(conf=conf)

# 加载 user_balance_table 数据
data_path = "/home/liuziyi/bigdata/user_balance_table.csv" 
raw_data = sc.textFile(data_path)

header = raw_data.first()
data = raw_data.filter(lambda line: line != header)

def parse_line(line):
    parts = line.split(",")
    date = parts[1]
    inflow = float(parts[4])  # 资金流入量
    outflow = float(parts[8])  # 资金流出量
    return (date, (inflow, outflow))

result = (
    data.map(parse_line)  # 解析每一行
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))  # 按日期聚合
        .sortByKey()  # 按日期排序
)
# 按日期聚合资金流入和流出
formatted_result = result.map(lambda x: f"{x[0]} {int(x[1][0])} {int(x[1][1])}")

# 在终端结果
for line in formatted_result.collect():
    print(line)

output_path = "/home/liuziyi/wxw/FundInOut.csv"
with open(output_path, 'w') as f:
    f.write("date,inflow,outflow\n")
    for date, (inflow, outflow) in result.collect():
        f.write(f"{date},{int(inflow)},{int(outflow)}\n")

#output_path = "/home/coco463/bigdata_hw/lab4/FundInOut_out_single"  
#formatted_result.coalesce(1).saveAsTextFile(output_path)
# 停止 SparkContext
sc.stop()