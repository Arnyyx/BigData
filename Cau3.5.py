from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Cho biết số lượng vận động viên nam tại mỗi kỳ thế vận hội trong thế kỷ 21") \
    .getOrCreate()
    
vdv_olympics_df = spark.read.option("header", "true",).csv("D:/Y3/BigData/BigData/DeTaiSo14/Data/vdv_olympics.csv")

filtered_df = vdv_olympics_df.filter((vdv_olympics_df["Year"] >= 2000) & (vdv_olympics_df["Sex"] == "M"))
male_athlete_count_by_year = filtered_df.groupBy("Year").agg(countDistinct("ID").alias("number_of_athletes")).orderBy("Year")
male_athlete_count_by_year.show()

spark.stop()