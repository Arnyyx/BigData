from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Cho biết chiều cao tối thiểu, \
            trung bình và tối đa của mỗi quốc gia tham gia thế vận hội mùa đông \
            và sắp xếp theo thứ tự giảm dần.") \
    .getOrCreate()
    
vdv_olympics_df = spark.read.option("header", "true",).csv("D:/Y3/BigData/BigData/DeTaiSo14/Data/vdv_olympics.csv")
qg_noc_df = spark.read.option("header", "true").csv("D:\Y3\BigData\BigData\DeTaiSo14\Data\qg_noc.csv")

joined_df = vdv_olympics_df.join(qg_noc_df, "NOC", "left").select(vdv_olympics_df["*"], qg_noc_df["Country"].alias("Country")).drop("NOC")
winter_df = joined_df.filter((joined_df["Season"] == "Winter") & (joined_df["Height"] != "NA"))

height_stats_by_noc = winter_df.groupBy("Country").agg(
    min("Height").alias("min_height"),
    format_number(avg("Height"), 2).alias("avg_height"),
    max("Height").alias("max_height")
).orderBy(desc("avg_height"))

height_stats_by_noc.show(height_stats_by_noc.count(), False)