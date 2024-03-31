import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Đưa ra thông tin về các vận động viên Việt Nam tham gia Olympic trong thập kỷ 2000") \
    .getOrCreate()

vdv_olympics_df = spark.read.csv(r'D:\Y3\BigData\BigData\DeTaiSo14\Data\vdv_olympics.csv', header=True, inferSchema=True)
qg_noc_df = spark.read.csv(r'D:\Y3\BigData\BigData\DeTaiSo14\Data\qg_noc.csv', header=True, inferSchema=True)

filtered_df = vdv_olympics_df.filter((vdv_olympics_df["NOC"] == "VIE") & (vdv_olympics_df["Year"].cast("int").between(2000, 2009)))

filtered_df.show(filtered_df.count(), False)
vdv_olympics_df.select()

spark.stop()
