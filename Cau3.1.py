from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Cho biết schema của bộ dữ liệu, hiển thị số bản ghi của bộ dữ liệu") \
    .getOrCreate()

vdv_olympics_df = spark.read.csv(r'D:\Y3\BigData\BigData\DeTaiSo14\Data\vdv_olympics.csv', header=True, inferSchema=True)
qg_noc_df = spark.read.csv(r'D:\Y3\BigData\BigData\DeTaiSo14\Data\qg_noc.csv', header=True, inferSchema=True)

print("Schema của bộ dữ liệu vdv_olympics.csv:")
vdv_olympics_df.printSchema()
print("Số bản ghi trong bộ dữ liệu:", vdv_olympics_df.count())

print("==========================================\n")

print("Schema của bộ dữ liệu qg_noc.csv:")
qg_noc_df.printSchema()
print("Số bản ghi trong bộ dữ liệu:", qg_noc_df.count())

spark.stop()
