from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct

spark = SparkSession.builder \
    .appName("Đưa ra thông tin về 7 quốc gia có số lượng vận đông viên đông nhất tham gia thế vận hội 2016") \
    .getOrCreate()

vdv_olympics_df = spark.read.csv(r'D:\Y3\BigData\BigData\DeTaiSo14\Data\vdv_olympics.csv', header=True, inferSchema=True)
qg_noc_df = spark.read.csv(r'D:\Y3\BigData\BigData\DeTaiSo14\Data\qg_noc.csv', header=True, inferSchema=True)

year_filtered_df = vdv_olympics_df.filter((vdv_olympics_df["Year"] == "2016"))
country_count_df = year_filtered_df.groupBy("NOC").agg(countDistinct("ID").alias("AthleteCount"))
top_7_countries = country_count_df.orderBy(country_count_df["AthleteCount"].desc()).limit(7)
top_7_countries.show(top_7_countries.count(), False)

spark.stop()
