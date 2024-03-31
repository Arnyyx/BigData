from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Final") \
    .getOrCreate()

vdv_olympics_df = spark.read.csv(r'D:\Y3\BigData\BigData\DeTaiSo14\Data\vdv_olympics.csv', header=True, inferSchema=True)
qg_noc_df = spark.read.csv(r'D:\Y3\BigData\BigData\DeTaiSo14\Data\qg_noc.csv', header=True, inferSchema=True)

print("Cau 3.1: Cho biết schema của bộ dữ liệu, hiển thị số bản ghi của bộ dữ liệu")
print("Schema của bộ dữ liệu vdv_olympics.csv:")
vdv_olympics_df.printSchema()
print("Số bản ghi trong bộ dữ liệu:", vdv_olympics_df.count())

print("Schema của bộ dữ liệu qg_noc.csv:")
qg_noc_df.printSchema()
print("Số bản ghi trong bộ dữ liệu:", qg_noc_df.count())
print("==========================================\n")

print("Cau 3.2: Đưa ra thông tin về các vận động viên Việt Nam tham gia Olympic trong thập kỷ 2000")
filtered_df = vdv_olympics_df.filter((vdv_olympics_df["NOC"] == "VIE") & (vdv_olympics_df["Year"].cast("int").between(2000, 2009)))
filtered_df.show(filtered_df.count(), False)
vdv_olympics_df.select()

spark.stop()
