from pyspark.sql.types import StructType, StructField, StringType
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
# Define schema cho DataFrame
schema = StructType([
    StructField("ID", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Sex", StringType(), True),
    StructField("Age", StringType(), True),
    StructField("Height", StringType(), True),
    StructField("Weight", StringType(), True),
    StructField("Team", StringType(), True),
    StructField("NOC", StringType(), True),
    StructField("Games", StringType(), True),
    StructField("Year", StringType(), True),
    StructField("Season", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Sport", StringType(), True),
    StructField("Event", StringType(), True),
    StructField("Medal", StringType(), True)
])

spark = SparkSession.builder.appName("Streaming").getOrCreate()
    
streaming_df = spark.readStream.schema(schema).option("header", "true").format("csv")\
.option("path", "D:\Y3\BigData\BigData\DeTaiSo14\Streaming").load()

transformed_df = streaming_df.filter("Medal = 'Gold'")
selected_df = transformed_df.select("ID", "Name", "Medal")

query = selected_df.writeStream.outputMode("append").format("console").start()

query.awaitTermination()
