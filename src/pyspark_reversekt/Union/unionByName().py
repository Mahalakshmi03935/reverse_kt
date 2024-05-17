from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("unionByName").getOrCreate()

data1 =  [(1, 'Raja', 'male'), (2, 'Ammu', 'Female')]
schema1 = ['id', 'name', 'gender']
df1 = spark.createDataFrame (data1, schema1)
print("df1:",df1)
df1.show()

data2= [(1, 'Raja', 'male','Switzerland'), (3, 'Maha', 'female','Kerala'), (4, 'Lakshmi', 'female','Chennai')]
schema2 = ['id', 'name', 'gender','place']
df2 = spark.createDataFrame (data2, schema2)
print("df2:",df2)
df2.show()


newDf = df1.unionByName(df2,allowMissingColumns=True)
print("unionByName:",newDf)
newDf.show()

