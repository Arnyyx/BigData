from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Cho biết tổng số vận động viên tại mỗi kỳ thế vận hội trong thập kỷ 1990 của Nga") \
    .getOrCreate()
    
vdv_olympics_df = spark.read.option("header", "true",).csv("D:/Y3/BigData/BigData/DeTaiSo14/Data/vdv_olympics.csv")

filtered_df = vdv_olympics_df.filter((vdv_olympics_df["Year"].cast("int").between(1990, 1999)) & (vdv_olympics_df["NOC"] == "RUS"))
athlete_count_by_year = filtered_df.groupby("Year", "Season").agg(countDistinct("ID").alias("number_of_athletes")).orderBy("Year")

athlete_count_by_year.show()