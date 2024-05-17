from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, collect_list, collect_set, countDistinct, count, first, last

spark = SparkSession.builder.appName("agg_func").getOrCreate()

data = [(1, "Raja" , 13),
        (2, "Maha",14),
        (3, "Maha",15),
        (4, "Tharani",16),
        (5, "Divya",17)]
df = spark.createDataFrame(data, ["id", "Name" , "Age"])
df.show()

# Mean
mean_result = df.select(mean("id")).collect()
print("Mean of the given data :", mean_result)

# Collect_list
collect_list_result = df.agg(collect_list("Name")).collect()
print("Collect List :", collect_list_result)

# Collect_set
collect_set_result = df.agg(collect_set("Name")).collect()
print("Collect Set:", collect_set_result)

# CountDistinct
count_distinct_result = df.select(countDistinct("Name")).collect()
print("Count Distinct:", count_distinct_result)

# Count
count_result = df.count()
print("Count:", count_result)

# First
first_result = df.select(first("Age")).collect()
print("First:", first_result)

# Last
last_result = df.select(last("Age")).collect()
print("Last:", last_result)
