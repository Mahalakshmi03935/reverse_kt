# Import SparkSession
from pyspark.sql import SparkSession

# Create a Spark Session
spark = SparkSession.builder \
    .appName("Spark Job Stage Task") \
    .getOrCreate()

# Read a CSV file (this is a transformation and doesn't trigger a job)
data = spark.read.option("header", "true").csv("C:\Users\RajaMahalakshmiB\Desktop\Reverse_kt\resources\csv file")

# Perform a transformation to create a new DataFrame with an added column
# This also doesn't trigger a job, as it's a transformation (not an action)
transformedData = data.withColumn("new_column", data["existing_column"] * 2)

# Now, call an action - this triggers a Spark job
result = transformedData.count()

print(result)

# Stop the Spark Session
spark.stop()
