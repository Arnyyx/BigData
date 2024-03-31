from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Number of Athletes in 5 Most Recent Olympic Games") \
    .getOrCreate()
    
vdv_olympics_df = spark.read.csv(r'D:\Y3\BigData\BigData\DeTaiSo14\Data\vdv_olympics.csv', header=True, inferSchema=True)
qg_noc_df = spark.read.csv(r'D:\Y3\BigData\BigData\DeTaiSo14\Data\qg_noc.csv', header=True, inferSchema=True)

male_athletes_by_year = vdv_olympics_df.filter(col('Sex') == 'M').groupBy('Year').agg(count('*').alias('number_of_athletes'))
print("Số lượng vận động viên nam tham gia mỗi kỳ Thế vận hội trong thế kỷ 21:")
male_athletes_by_year.show()

spark.stop()
