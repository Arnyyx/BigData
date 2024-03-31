from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Liệt kê danh sách 10 quốc gia giành nhiều huy chương vàng nhất thế vận hội mùa hè 2012") \
    .getOrCreate()
    
vdv_olympics_df = spark.read.csv(r'D:\Y3\BigData\BigData\DeTaiSo14\Data\vdv_olympics.csv', header=True, inferSchema=True)
qg_noc_df = spark.read.csv(r'D:\Y3\BigData\BigData\DeTaiSo14\Data\qg_noc.csv', header=True, inferSchema=True)

top_10_gold_medal_countries_2012 = vdv_olympics_df.filter((col('Year') == 2012) & (col('Season') == 'Summer') & (col('Medal') == 'Gold')).groupBy('NOC').agg(count('*').alias('gold_medals_count')).orderBy('gold_medals_count', ascending=False).limit(10)
print("Danh sách 10 quốc gia giành nhiều huy chương vàng nhất")
top_10_gold_medal_countries_2012.show()
