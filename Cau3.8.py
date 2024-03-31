from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Đưa ra tên các quốc gia không có thông tin về quốc gia trong qg_noc \
             (ví dụ: Yugoslavia-Nam Tư không còn tồn tại nữa)") \
    .getOrCreate()
    
vdv_olympics_df = spark.read.csv(r'D:\Y3\BigData\BigData\DeTaiSo14\Data\vdv_olympics.csv', header=True, inferSchema=True)
qg_noc_df = spark.read.csv(r'D:\Y3\BigData\BigData\DeTaiSo14\Data\qg_noc.csv', header=True, inferSchema=True)

missing_country_info = qg_noc_df.filter(qg_noc_df['Population'].isNull() | qg_noc_df['GDP per Capita'].isNull())
print("Tên các quốc gia không có thông tin về quốc gia trong qg_noc:")

missing_country_info.drop("Population").drop("GDP per Capita").show()
