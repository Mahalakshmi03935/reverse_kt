from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, min, max, round, abs

spark = SparkSession.builder.appName("numeric_func").getOrCreate()

data = [(1, 10.567543567543,-3),
        (2, -5.3, 7),
        (3, 8.9, 4),
        (4, 0.1, -2),
        (5, -12.74675368775, 0)]
df = spark.createDataFrame(data, ["id", "value1", "value2"])
df.show()

# Sum()
sum_result = df.select(sum("value1")).collect()
print("Sum of value1:", sum_result)

# Avg()
avg_result = df.select(avg("value1")).collect()
print("Average of value1:", avg_result)

# Min()
min_result = df.select(min("value1")).collect()
print("Minimum of value1:", min_result)

# Max()
max_result = df.select(max("value1")).collect()
print("Maximum of value1:", max_result)

# Round()
round_result = df.select(round("value1", 2)).collect()
print("Rounded values of value1:", round_result)

# Abs()
abs_result = df.select(abs("value2")).collect()
print("Absolute values of value2:",  abs_result)

