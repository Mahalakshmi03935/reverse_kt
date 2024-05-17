from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("union").getOrCreate()

data1 =  [(1, 'Raja', 'male'), (2, 'Ammu', 'Female')]
schema1 = ['id', 'name', 'gender']
df1 = spark.createDataFrame (data1, schema1)
print("df1:",df1)
df1.show()

data2= [(1, 'Raja', 'male'), (3, 'Maha', 'female'), (4, 'Lakshmi', 'female')]
schema2 = ['id', 'name', 'gender']
df2 = spark.createDataFrame (data2, schema2)
print("df2:",df2)
df2.show()


newDf = df1.union(df2)
print("union:",newDf)
newDf.distinct().show()

