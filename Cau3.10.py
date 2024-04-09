from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Liệt kê 10 vận động viên có giành nhiều huy chương vàng nhất thế kỷ 20") \
    .getOrCreate()
    
vdv_olympics_df = spark.read.csv(r'D:\Y3\BigData\BigData\DeTaiSo14\Data\vdv_olympics.csv', header=True, inferSchema=True)
qg_noc_df = spark.read.csv(r'D:\Y3\BigData\BigData\DeTaiSo14\Data\qg_noc.csv', header=True, inferSchema=True)

top_10_athletes_20th_century = vdv_olympics_df.filter((col('Year') >= 1900) & (col('Year') <= 2000) & (col('Medal') == 'Gold')).groupBy('Name').agg(count('*').alias('gold_medals_count')).orderBy('gold_medals_count', ascending=False).limit(10)
print("Danh sách 10 vận động viên giành nhiều huy chương vàng nhất thế kỷ 20:")
top_10_athletes_20th_century.show()
