from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Số lượng vận động viên tham dự 5 kỳ thế vận hội gần nhất") \
    .getOrCreate()
    
vdv_olympics_df = spark.read.csv(r'D:\Y3\BigData\BigData\DeTaiSo14\Data\vdv_olympics.csv', header=True, inferSchema=True)
qg_noc_df = spark.read.csv(r'D:\Y3\BigData\BigData\DeTaiSo14\Data\qg_noc.csv', header=True, inferSchema=True)

athlete_counts_by_year = vdv_olympics_df.groupBy("Games").agg(countDistinct("ID").alias("AthleteCount"))
recent_olympics = athlete_counts_by_year.orderBy("Games", ascending=False).limit(5)
recent_olympics.show()

spark.stop()
