from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("BT") \
    .getOrCreate()

file_path1 = r"D:\Y3\BigData\BigData\DeTaiSo14\Data\qg_noc.csv" 
file_path2 = r"D:\Y3\BigData\BigData\DeTaiSo14\Data\vdv_olympics.csv" 

df1 = spark.read.csv(file_path1, header=True, inferSchema=True)
df2 = spark.read.csv(file_path2, header=True, inferSchema=True)

print("Câu 1:")
print("qg_noc: ")
df1.printSchema()
print("Số bản ghi: ", df1.count()) 
print("vdv_olympic: ")
df2.printSchema()
print("Số bản ghi: ", df2.count()) 

print("Câu 2:")
cacvandongvienVN = df2.filter((col("NOC") == "VIE") & (col("Year") >= 2000) & (col("Year") <= 2009))
print("Các vận động viên Việt Nam tham gia Olympic trong thập ký 2000: ")
cacvandongvienVN.show()

print("Câu 3:")
bayquocgiacosoluongvdvdongnhat2016=df2.filter(col("year")==2016).groupBy("Team").count().orderBy("count", ascending=False).limit(7)
print("Đưa ra thông tin về 7 quốc gia có số lượng vận đông viên đông nhất tham gia thế vận hội 2016: ")
bayquocgiacosoluongvdvdongnhat2016.show()
print("câu 4:")
soluongvandongvienthamgia5kygannhat=df2.groupBy("Year").count().orderBy("Year", ascending=False).limit(5)
print("Số lượng vận động viên tham dự 5 kỳ thế vận hội gần nhất:")
soluongvandongvienthamgia5kygannhat.show()

print("Câu 5:")
soluongvandongvientaimoikythevanhoitheky21=df2.filter((col("Sex")=="M")  & (col("Year")>=2000) & (col("Year")<= 2099) )
print("Số lượng vận động viên nam tại mỗi kỳ thế vận hội trong thế kỷ 21: ",soluongvandongvientaimoikythevanhoitheky21.count())

print("Câu 6:")
tongsovandongvientaimoikytrongthapky1990oNga=df2.filter((col("NOC")=="RUS")).groupBy("Season").count()
print("Tổng số vận động viên tại mỗi kỳ thế vận hội trong thập kỷ 1990 của Nga: ")
tongsovandongvientaimoikytrongthapky1990oNga.show()

print("Câu 7:")
athlete_height_stats = df2.groupby('NOC').agg({'Height': ['min', 'avg', 'max']}).reset_index()
athlete_height_stats.columns = ['NOC', 'min_height', 'avg_height', 'max_height']
athlete_height_stats_winter = athlete_height_stats.sort_values(by='max_height', ascending=False)
print("Chiều cao tối thiểu, trung bình và tối đa của mỗi quốc gia tham gia Thế vận hội mùa đông và sắp xếp theo thứ tự giảm dần:")
print(athlete_height_stats_winter)

print("Câu 8:")
missing_country_info = df1[df1['Population'].isnull()]
print("Tên các quốc gia không có thông tin về quốc gia trong qg_noc:")
print(missing_country_info)

print("Câu 9:")
top_10_gold_medal_countries_2012 = df2[(df2['Year'] == 2012) & (df2['Season'] == 'Summer') & (df2['Medal'] == 'Gold')].groupby('NOC')['Medal'].count().nlargest(10)
print("Danh sách 10 quốc gia giành nhiều huy chương vàng nhất Thế vận hội mùa hè 2012:")
print(top_10_gold_medal_countries_2012)

print("Câu 10:")
top_10_athletes_20th_century = df2[(df2['Year'] >= 1900) & (df2['Year'] <= 2000) & (df2['Medal'] == 'Gold')]['Name'].value_counts().nlargest(10)
print("Danh sách 10 vận động viên giành nhiều huy chương vàng nhất thế kỷ 20:")
print(top_10_athletes_20th_century)

print("Bai 4:")
gold_medalists = df2[(df2['Year'] == 2016) & (df2['Medal'] == 'Gold')]
print("Lọc và hiển thị thông tin về các vận động viên giành huy chương vàng từ dữ liệu vận động viên tại Thế vận hội 2016:")
print(gold_medalists[['ID', 'Name', 'Sex', 'Age', 'Height', 'Weight', 'Team', 'NOC', 'Games', 'Year', 'Season', 'City', 'Sport', 'Event', 'Medal']])
