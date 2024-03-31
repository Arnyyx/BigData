from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Cho biết số lượng vận động viên nam tại mỗi kỳ thế vận hội trong thế kỷ 21") \
    .getOrCreate()
    
df = spark.read.option("header", "true",).csv("D:/Y3/BigData/BigData/DeTaiSo14/Data/vdv_olympics.csv")

filtered_df = df.filter((df["Year"] >= 2000) & (df["Sex"] == "M"))

male_athlete_count_by_year = filtered_df.groupBy("Year").agg(countDistinct("ID").alias("number_of_athletes")).orderBy("Year")
male_athlete_count_by_year.show()